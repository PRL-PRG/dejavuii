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
            bool valid;
            bool tainted;

            
            Commit(unsigned id, uint64_t time, uint64_t time2):
                BaseCommit<Commit>(id, time),
                time2(time2),
                valid(true),
                tainted(false) {
            }

            bool verifyTimings() {
                for (Commit * c : parents) {
                    if (c->time > time || c->time2 > time2) {
                        valid = false;
                        return false;
                    }
                }
                return true;
            }
            
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            std::string user;
            std::string repo;
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt),
                user(user),
                repo(repo) {
            }

        };

        /** Tracks known files in a project.
         */
        class ProjectState {
        public:
            ProjectState() {
            }

            ProjectState(ProjectState const & other) {
                mergeWith(other, nullptr);
            }

            void mergeWith(ProjectState const & other, Commit * c) {
                for (auto i : other.files_)
                    files_.insert(i);
            }

            bool verifyCommit(Commit * c) {
                for (auto i : c->deletions) {
                    auto j = files_.find(i);
                    if (j == files_.end())
                        return false;
                    files_.erase(j);
                }
                for (auto i : c->changes)
                    files_.insert(i.first);
                return true;
            }
        private:
            std::unordered_set<unsigned> files_;
        };

        /**
         */
        class Verifier {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        if (id >= projects_.size())
                            projects_.resize(id + 1);
                        projects_[id] = new Project(id, user, repo, createdAt);
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        if (id >= commits_.size())
                            commits_.resize(id + 1);
                        commits_[id] = new Commit(id, authorTime, committerTime);
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
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                    }};
            }

            void verifyProjectStructure() {
                std::vector<std::thread> threads;
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, this]() {
                        while (true) {
                            size_t i = 0;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size())
                                    return;
                                i = completed;
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            Project * p = projects_[i];
                            if (p == nullptr)
                                continue;
                            if (! verifyProjectStructure(p)) {
                                std::lock_guard<std::mutex> g(mCerr_);
                                failedStructure_.push_back(p);
                                projects_[i] = nullptr;
                                std::cerr << "    failed project " << p->id << ": " << p->user << "/" << p->repo << std::endl;
                            }
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "    TOTAL: " << failedStructure_.size() << " failed projects" << std::endl;
            }

            void verifyCommitTimings() {
                size_t failed = 0;
                std::cerr << "Verifying commit timings ..." << std::endl;
                for (Commit * c : commits_) {
                    if (c == nullptr)
                        continue;
                    if (! c->verifyTimings())
                        ++failed;
                }
                std::cerr << "    " << failed << " failed commits" << std::endl;
                for (size_t i = 0, e = projects_.size(); i != e; ++i) {
                    Project * p = projects_[i];
                    if (p == nullptr)
                        continue;
                    bool valid = true;
                    for (Commit * c : p->commits)
                        if (! c->valid) {
                            valid = false;
                            break;
                        }
                    if (!valid) {
                        projects_[i] = nullptr;
                        failedTimings_.push_back(p);
                    }
                }
                std::cerr << "    " << failedTimings_.size() << " affected projects" << std::endl;
                std::cerr << "Calculating surviving commits..." << std::endl;
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    for (Commit * c : p->commits) {
                        assert(c->valid);
                        c->tainted = true;
                    }
                        
                }
                    
            }

            void outputResults() {
                helpers::EnsurePath(OutputDir.value());
                {
                    std::cerr << "Writing projects..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/projects.csv");
                    f << "#projectId,user,repo,createdAt" << std::endl;
                    for (auto i : projects_) {
                        if (i == nullptr)
                            continue;
                        f << i->id << "," << helpers::escapeQuotes(i->user) << "," << helpers::escapeQuotes(i->repo) << "," << i->createdAt << std::endl;
                    }
                }
                {
                    std::cerr << "Writing commits..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commits.csv");
                    f << "#commitId,authorTime,committerTime" << std::endl;
                    for (auto i : commits_) {
                        if (i == nullptr)
                            continue;
                        if (! i->tainted)
                            continue;
                        f << i->id << "," << i->time << "," << i->time2 << std::endl;
                    }
                }
                {
                    std::cerr << "Writing commit parents..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commitParents.csv");
                    f << "#commitId,parentId" << std::endl;
                    for (Commit * c : commits_) {
                        if (c == nullptr)
                            continue;
                        if (! c->tainted)
                            continue;
                        for (Commit * p : c->parents)
                            f << c->id << "," << p->id << std::endl;
                    }
                }
                {
                    std::cerr << "Writing file changes..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/fileChanges.csv");
                    f << "#projectId,commitId,pathId,contentsId" << std::endl;
                    for (Project * p : projects_) {
                        if (p == nullptr)
                            continue;
                        for (Commit * c : p->commits) {
                            for (unsigned i : c->deletions)
                                f << p->id << "," << c->id << "," << i << "," << FILE_DELETED << std::endl;
                            for (auto i : c->changes)
                                f << p->id << "," << c->id << "," << i.first << "," << i.second << std::endl;
                        }
                    }
                }
            }

            void outputErrors() {
                std::cerr << "Writing project structure errors..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/projects_structureErrors.csv");
                    f << "#projecdId" << std::endl;
                    for (Project * p : failedStructure_)
                        f << p->id << std::endl;
                }
                std::cerr << "Writing commit timings errors..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/projects_timingsErrors.csv");
                    f << "#projecdId" << std::endl;
                    for (Project * p : failedTimings_)
                        f << p->id << std::endl;
                }
                {
                    std::ofstream f(DataDir.value() + "/commits_timingsErrors.csv");
                    f << "#commitId" << std::endl;
                    for (Commit * c : commits_) {
                        if (c == nullptr || c->valid)
                            continue;
                        f << c->id << std::endl;
                    }
                }
            }

            /** Creates symlinks for non-filtered files from the original dataset to the verified one to save disk space.
             */
            void createSymlinks() {
                std::cerr << "Creating symlinks..." << std::endl;
                helpers::System(STR("ln -s " << DataDir.value() + "/paths.csv " << OutputDir.value() << "/paths.csv"));
                helpers::System(STR("ln -s " << DataDir.value() + "/hashes.csv " << OutputDir.value() << "/hashes.csv"));
                // TODO do we want more symlinks?
            }

        private:


            bool verifyProjectStructure(Project * p) {
                bool valid = true;
                CommitForwardIterator<Project,Commit,ProjectState> i(p, [&,this](Commit * c, ProjectState & state) {
                        if (!state.verifyCommit(c)) 
                            valid = false;
                        if (! valid)
                            return false;
                        return true;
                });
                i.process();
                return valid;
            }
            
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;
            /** Projects for which the structure in their commits does not make sense, i.e. we observe deletion of a file we did not observe being created.
             */
            std::vector<Project *> failedStructure_;

            /** Projects for which the times of the commits makes no sense (i.e. parent commits are younger than their children).

                TODO should also projects that are obviously too old be considered?
             */
            std::vector<Project *> failedTimings_;



            std::mutex mCerr_;
            
        }; // Verifier

    } // anonymous namespace


    void Verify(int argc, char * argv[]) {
        NumThreads.updateDefaultValue(8);
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Verifier v;
        v.loadData();
        v.verifyProjectStructure();
        v.verifyCommitTimings();
        v.outputResults();
        v.outputErrors();
        v.createSymlinks();
    }
    
} // namespace dejavu
