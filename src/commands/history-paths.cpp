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

namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time){
            }

        };

        class Project : public BaseProject<Project, Commit> {
        public:

            unsigned uniqueFiles = 0;
            unsigned originalFiles = 0;
            unsigned cloneFiles = 0;
            
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }
        };

        class FileOriginal {
        public:
            unsigned id;
            Project * project;
            Commit * commit;
            unsigned fileId;
            unsigned numOccurences;

            FileOriginal(unsigned id, Project * project, Commit * commit, unsigned fileId):
                id(id),
                project(project),
                commit(commit),
                fileId(fileId),
                numOccurences(1) {
            }

            void update(Project * project, Commit * commit, unsigned fileId) {
                ++this->numOccurences;
                // if the new commit is newer, nothing to do
                if (commit->time > this->commit->time)
                    return;
                if (commit->time == this->commit->time) {
                    // if the commit is same age, but the project is newer or same, nothing to do
                    if (project->createdAt >= this->project->createdAt)
                        return;
                }
                this->project = project;
                this->commit = commit;
                this->fileId = fileId;
            }
        };


        enum FileState {
            Unique,
            Original,
            Clone,
        };
        
        class FileInfo {
        public:
            unsigned pathId;
            unsigned uniqueCommits;
            unsigned originalCommits;
            unsigned cloneCommits;
            unsigned deletions;
            FileState initialState;
            FileState lastState;

            FileInfo(unsigned pathId,FileState state):
                pathId(pathId),
                uniqueCommits(0),
                originalCommits(0),
                cloneCommits(0),
                deletions(0),
                initialState(state) {
                addCommit(state);
            }

            void addCommit(FileState state) {
                switch (state) {
                case FileState::Unique:
                    ++uniqueCommits;
                    break;
                case FileState::Original:
                    ++originalCommits;
                    break;
                case FileState::Clone:
                    ++cloneCommits;
                    break;
                }
            }
        };

        class State {
        public:
            State() {
            }

            State(State const & from) {
                mergeWith(from, nullptr);
            }

            void mergeWith(State const & other, Commit * c) {
                files.insert(other.files.begin(), other.files.end());
            }

            std::unordered_map<unsigned, FileInfo *> files;
            
        }; // State

        

        class PathsCounter {
        public:
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
                std::cerr << "Loading file changes ... " << std::endl;
                size_t numChanges = 0;
                size_t numDeletions = 0;
                FileChangeLoader{[& numChanges, & numDeletions, this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        if (contentsId == FILE_DELETED) {
                            ++numDeletions;
                        } else {
                            ++numChanges;
                            auto i = originals_.find(contentsId);
                            if (i != originals_.end())
                                i->second->update(p, c, pathId);
                            else
                                originals_.insert(std::make_pair(contentsId, new FileOriginal(contentsId, p, c, pathId)));
                        }
                    }};
                std::cerr << "    " << numDeletions << " deletions" << std::endl;
                std::cerr << "    " << numChanges << " changes" << std::endl;
                std::cerr << "    " << originals_.size() << " contents hashes" << std::endl;
                
            }

            /** This removes the unique files from the list so that we don't bother with these.
             */
            void removeUniqueFiles() {
                std::cerr << "Removing unique files..." << std::endl;
                for (auto i = originals_.begin(); i != originals_.end();) {
                    if (i->second->numOccurences == 1)
                        i = originals_.erase(i);
                    else
                        ++i;
                }
                std::cerr << "    " << originals_.size() << " non-unique content hashes (originals)" << std::endl;
            }

            void calculatePathsHistory() {
                std::cerr << "Analyzing clone behavior..." << std::endl;
                std::vector<std::thread> threads;
                auto i = projects_.begin();
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, this]() {
                        while (true) {
                            Project * p;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (i == projects_.end())
                                    return;
                                p = i->second;
                                ++i;
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

            void outputProjectsAggregate() {
                std::cerr << "Writing project aggregates..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectPaths.csv");
                f << "projectId,uniqueFiles,originalFiles,cloneFiles" << std::endl;
                size_t uniqueFiles = 0;
                size_t originalFiles = 0;
                size_t cloneFiles = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->uniqueFiles << "," << p->originalFiles << "," << p->cloneFiles << std::endl;
                    uniqueFiles += p->uniqueFiles;
                    originalFiles += p->originalFiles;
                    cloneFiles += p->cloneFiles;
                }
                std::cerr << "    " << uniqueFiles << " unique files" << std::endl;
                std::cerr << "    " << originalFiles << " original files" << std::endl;
                std::cerr << "    " << cloneFiles << " clone files" << std::endl;
            }
            
        private:

            /** Analyzes the project for unique files and their changing commits across the history of the project.
             */
            void analyzeProject(Project * p) {
                std::unordered_map<unsigned, FileInfo *> files;
                CommitForwardIterator<Project, Commit, State> it(p, [&, this](Commit * c, State & state){
                        // first handle all deletions
                        for (unsigned pathId : c->deletions) {
                            ++state.files[pathId]->deletions;
                            state.files.erase(pathId);
                        }
                        // and now add changes
                        for (auto ch : c->changes) {
                            auto i = state.files.find(ch.first);
                            // if not in active files, check project files, otherwise create
                            if (i == state.files.end()) {
                                i = files.find(ch.first);
                                if (i == files.end()) {
                                    FileInfo * fi = new FileInfo(ch.first, getFileState(p, c, ch.first, ch.second));
                                    files.insert(std::make_pair(ch.first, fi));
                                    state.files.insert(std::make_pair(ch.first, fi));
                                } else {
                                    i->second->addCommit(getFileState(p, c, ch.first, ch.second));
                                    state.files.insert(*i);
                                }
                            // otherwise just register the commit
                            } else {
                                i->second->addCommit(getFileState(p, c, ch.first, ch.second));
                            }
                        }
                        return true;
                   });
                it.process();
                // update the project with aggregates
                for (auto i : files) {
                    FileInfo * fi = i.second;
                    if (fi->cloneCommits > 0)
                        ++ p->cloneFiles;
                    else if (fi->originalCommits > 0)
                        ++ p->originalFiles;
                    else
                        ++ p->uniqueFiles;
                }
            }

            FileState getFileState(Project * p, Commit * c, unsigned pathId, unsigned contentsId) {
                auto i = originals_.find(contentsId);
                if (i == originals_.end())
                    return FileState::Unique;
                if (i->second->project == p && i->second->commit == c && i->second->fileId == pathId)
                    return FileState::Original;
                return FileState::Clone;
            }


            std::mutex mCerr_;
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, FileOriginal *> originals_;
            
        }; // PathsCounter
    }

    void HistoryPaths(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        PathsCounter pc;
        pc.loadData();
        pc.removeUniqueFiles();
        pc.calculatePathsHistory();
        pc.outputProjectsAggregate();
    }

    
}
