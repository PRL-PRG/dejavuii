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
                BaseCommit<Commit>(id, time) {
            }
        };

        class Project : public BaseProject<Project, Commit> {
        public:
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
            unsigned numClones;

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

        class FileClone {
        public:
            unsigned projectId;
            unsigned commitId;
            unsigned pathId;
            unsigned cloneId;

            FileClone(unsigned projectId, unsigned commitId, unsigned pathId, unsigned cloneId):
                projectId(projectId),
                commitId(commitId),
                pathId(pathId),
                cloneId(cloneId) {
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

            // pathId -> contentsId
            std::unordered_map<unsigned, unsigned> files;
                
        };

        class FileClonesDetector {
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
                std::cerr << "    " << originals_.size() << " unique contents (possible originals)" << std::endl;
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
                std::cerr << "    " << originals_.size() << " non-unique file contents left" << std::endl;
            }

            /** Looks for clone candidates in all loaded projects.
             */
            void detectClones() {
                std::cerr << "Analyzing projects for clone candidates..." << std::endl;
                clonesOut_ = std::ofstream(DataDir.value() + "/fileCloneCandidates.csv");
                clonesOut_ << "cloneId,projectId,commitId,pathId" << std::endl;
                std::vector<std::thread> threads;
                auto i = projects_.begin();
                size_t completed = 0;
                size_t numClones = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, &numClones, this]() {
                        std::vector<FileClone> clones;
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
                            detectClonesInProject(p, clones);
                            if (!clones.empty()) {
                                {
                                    std::lock_guard<std::mutex> g(mClonesOut_);
                                    numClones += clones.size();
                                }
                                clones.clear();
                            }
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "    " << numClones << " file clones detected" << std::endl;
            }
            
            


        private:


            void detectClonesInProject(Project * p, std::vector<FileClone> & clones) {
                CommitForwardIterator<Project, Commit, State> i(p, [&, this](Commit * c, State & state){
                        // first remove all files added by the commit
                        for (unsigned pathId : c->deletions) {
                            //assert(state.files.find(pathId) != state.files.end());
                            std::cout << p->id << "," << c->id << "," << pathId << std::endl;
                            state.files.erase(pathId);
                        }
                        // now for each change, update the change and determine if the thing is a clone
                        for (auto i : c->changes) {
                            // let's see if there is an original
                            auto o = originals_.find(i.second);
                            if (o != originals_.end()) {
                                // ignore intra-project clones
                                if (o->second->project != p) 
                                    clones.push_back(FileClone(p->id, c->id, i.first, i.second));
                            }
                            // add the change
                            state.files[i.first] = i.second;
                        }
                        return true;
                    });
                i.process();
            }

            std::mutex mCerr_;

            std::mutex mClonesOut_;
            std::ofstream clonesOut_;
            
            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, FileOriginal *> originals_;
        };


        
    } // anonymous namespace

    void DetectFileClones(int argc, char * argv[]) {
        Threshold.updateDefaultValue(2);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        FileClonesDetector fcd;
        fcd.loadData();
        fcd.removeUniqueFiles();
        fcd.detectClones();
    }


    
} // namespace dejavu 
