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
            uint64_t time2;
            Commit(unsigned id, uint64_t time, uint64_t time2):
                BaseCommit<Commit>(id, time),
                time2(time2){
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

        class FinalBreaker {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime, committerTime)));
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
                std::cerr << "    " << originals_.size() << " unique contents" << std::endl;
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
                std::cerr << "    " << originals_.size() << " originals (with at least one copy)" << std::endl;
            }
        private:

            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, FileOriginal *> originals_;


            
        }; // FinalBreaker
        
    } // anonymous namespace





    
    void FinalBreakdown(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        FinalBreaker fb;
        fb.loadData();
    }
    
} // namespace dejavu
