#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <openssl/sha.h>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

/** Filters given folder clones from the dataset.

    This is done by essentially loading the entire dataset and then going project by project, tagging those files which belong to a clone and then ignoring their changes until they are deleted.
 */

namespace dejavu {

    namespace {
        
        class Clone {
        public:
            unsigned id;
            unsigned projectId;
            unsigned commitId;
            std::string path;
            unsigned fileChanges;

            Clone(unsigned id, unsigned projectId, unsigned commitId, std::string const & path):
                id(id),
                projectId(projectId),
                commitId(commitId),
                path(path),
                fileChanges(0) {
            }


            
        }; // Clone

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }

        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }

            void addIgnoredChange(Commit * c, unsigned pathId) {
                ignoredChanges[c->id].insert(pathId);
            }

            // commit id -> clones it introduces
            std::unordered_map<unsigned, std::unordered_set<Clone *>> clones;

            // commit id -> set of pathIds that are to be ignored because they change a clone
            std::unordered_map<unsigned, std::unordered_set<unsigned>> ignoredChanges;

        };


        /** The state for the project iteration.
         */
        class State {
        public:
            /** Start with empty active clones.
             */
            State() = default;

            State(State const & from) {
                mergeWith(from);
            }

            void mergeWith(State const & from, Commit * c = nullptr) {
                for (auto i : from.activeClones) {
                    auto j = activeClones.find(i.first);
                    if (j == activeClones.end()) {
                        activeClones.insert(i);
                    } else {
                        for (auto ui : i.second)
                            j->second.insert(ui);
                    }
                }
            }

            /** Checks *all* active clones if they contain specified pathId and if they do, removes the */
            bool registerDeletion(unsigned pathId) {
                bool result = false;
                for (auto i = activeClones.begin(), e = activeClones.end(); i != e; ) {
                    if (i->second.erase(pathId) != 0) {
                        // the change is to be ignored
                        result = true;
                        // add number of file changes in the given clone
                        ++i->first->fileChanges;
                        // if the clone is now empty, remove the clone from the set of active clones
                        if (i->second.empty()) {
                            i = activeClones.erase(i);
                            continue;
                        }
                    }
                    ++i;
                }
                return result;
            }

            void addActiveClone(Clone * clone) {
                activeClones.insert(std::make_pair(clone, std::unordered_set<unsigned>()));
            }

            bool registerChange(unsigned pathId, std::string const & path) {
                bool result = false;
                for (auto i : activeClones) {
                    if (helpers::startsWith(path, i.first->path)) {
                        result = true;
                        i.second.insert(pathId);
                        ++i.first->fileChanges;
                    }
                }
                return result;
            }
            
            // clone prefix -> set of paths it contains that should be ignored
            std::unordered_map<Clone *, std::unordered_set<unsigned>> activeClones;
            
        }; // State



        class FolderCloneFilter {
        public:
            /** Loads the data required.
             */
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                    }};
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        paths_.insert(std::make_pair(id, path));
                    }};
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                    }};
                // finally, load the clone occurences we want to filter out
                FolderCloneOccurencesLoader(Filter.value(), [this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & path, unsigned numFiles) {
                        // add the clone to the particular commit.
                        projects_[projectId]->clones[commitId].insert(new Clone(cloneId, projectId, commitId, path));
                    });
            }

            void filterFileChanges() {
                std::cerr << "Filtering file changes..." << std::endl;
                std::vector<std::thread> threads;
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, this]() {
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size())
                                    return;
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            analyzeProject(p);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
            }

        private:

            /** Takes the project and creates a list of changes to be removed because they either create, or modify a clone.

                
             */
            void analyzeProject(Project * p) {
                // if there are no clones in the project, no need to go though it
                if (p->clones.empty())
                    return;
                CommitForwardIterator<Project,Commit,State> i(p, [&, this](Commit * c, State & state){
                        // first deal with deletions in the commit, if they belong to any active commit
                        if (!state.activeClones.empty()) {
                            for (unsigned pathId: c->deletions)
                                if (state.registerDeletion(pathId))
                                    p->addIgnoredChange(c, pathId);
                        }
                        // now take any clones added in the commit and add them to active clones

                        auto i = p->clones.find(c->id);
                        if (i != p->clones.end()) {
                            for (Clone * clone : i->second)
                                state.addActiveClone(clone);
                        }
                        // and now, take every change and determine if it belongs to any of the active clones
                        if (!state.activeClones.empty()) {
                            for (auto i : c->changes) {
                                if (state.registerChange(i.first, paths_[i.first]))
                                    p->addIgnoredChange(c, i.first);
                            }
                        }
                        // that's it
                        return true;
                    });
                i.process();
                std::cerr << "Project " << p->id << " (ignored changed: " << p->ignoredChanges.size() << ")" << std::endl;
                for (auto i : p->clones)
                    for (auto clone : i.second)
                        std::cerr << "    " << clone->path << " : " << clone->fileChanges << std::endl;
            }



            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, std::string> paths_;

            std::mutex mCerr_;

            
        }; // FolderCloneFilter
        
    } // anonymous namespace
    



    void FilterFolderClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(Filter);
        Settings.addOption(OutputDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        helpers::EnsurePath(OutputDir.value());
        FolderCloneFilter fcf;
        fcf.loadData();
        
    }
    
} // namespace dejavu
