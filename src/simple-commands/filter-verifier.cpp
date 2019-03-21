#include "simple-commands.h"

#include "../settings.h"
#include "../objects.h"



/*
73778 unaccounted projects
42383751 unaccounted commits
*/

namespace dejavu {

    namespace {

        class RedoException {
        };

        helpers::Option<std::string> InputDir("inputDir", "/input", false);
        helpers::Option<std::string> OutputDir("outputDir", "/verified", false);

        class Verifier {
        public:

            void verify() {
                // first load the projects and their already calculated metadata
                ProjectsLoader(*this, DataRoot.value() + InputDir.value() + "/projects.csv");
                while (true) {
                    try {
                        std::cout << "Projects: " << projects_.size() << std::endl;
                        for (auto i : commits_) {
                            delete i.second;
                        }
                        for (auto i : projects_)
                            i.second->commits_.clear();
                        commits_.clear();
                        hashToCommit_.clear();
                        std::cout << "Commits before reading : " << commits_.size() << " (" << hashToCommit_.size() << ")" << std::endl;
                        // now read through the commits we have and fill in the hashes
                        CommitsLoader(*this, DataRoot.value() + InputDir.value() + "/commits.csv");
                        // get the network of commits now to delete unused commits and unused projects
                        CommitNetworkLoader(*this, DataRoot.value() + InputDir.value() + "/selective-commit-network.csv");
                        // then look at the commit histories we have downloaded separately to fill in autor and commit times for the commits
                        CommitHistoryLoader(*this, DataRoot.value() + InputDir.value() + "/commit-history.txt");
                        // look at all the changes we have and make sure that data makes sense (i.e. commits are accounted for, projects too, etc.)
                        ChangesLoader(*this, DataRoot.value() + InputDir.value() + "/files.csv", DataRoot.value() + OutputDir.value() + "/changes.csv");
                        // now we are done with reading... Output what we have
                        writeProjects(DataRoot.value() + OutputDir.value() + "/projects.csv");
                        writeCommits(DataRoot.value() + OutputDir.value() + "/commits.csv");

                        PathsFilter(*this, DataRoot.value() + InputDir.value() + "/paths.csv", DataRoot.value() + OutputDir.value() + "/paths.csv");
                        FileHashFilter(*this, DataRoot.value() + InputDir.value() + "/fileHashes.csv", DataRoot.value() + OutputDir.value() + "/fileHashes.csv");
                        return;
                    } catch (RedoException const & e) {
                        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!! Redoing ! " << std::endl;
                    }
                }
            }

        private:

            class ProjectRecord;
            class CommitRecord;

            /** Loads the projects and their metadata.
             */
            class ProjectsLoader : public Project::Reader {
            public:
                ProjectsLoader(Verifier & v, std::string const & filename):
                    v_(v) {
                    std::cout << "Reading projects..." << std::endl;
                    readFile(filename, true);
                }

            protected:

                virtual void onRow(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt, int fork, unsigned committers, unsigned authors, unsigned watchers) {
                    ProjectRecord * p = v_.getOrCreateProject(id);
                    p->user = user;
                    p->repo = repo;
                    p->createdAt = createdAt;
                    p->fork = fork;
                    p->committers = committers;
                    p->authors = authors;
                    p->watchers = watchers;
                }

                void onDone(size_t numRows) {
                    std::cout << numRows << " projects loaded" << std::endl;
                }

            private:
                Verifier & v_;
            }; 

            /** Loads the commits from the commits.csv and fills in the commit record hash information.
             */
            class CommitsLoader : public Commit::Reader {
            public:
                CommitsLoader(Verifier & v, std::string const & filename):
                    v_(v) {
                    std::cout << "Reading commit hashes..." << std::endl;
                    readFile(filename, false);
                }

            protected:
                void onRow(unsigned id, std::string const & hash, uint64_t time) override {
                    CommitRecord * cr = v_.getOrCreateCommit(id);
                    if (cr != nullptr) {
                        cr->hash = hash;
                        v_.hashToCommit_.insert(std::make_pair(hash, cr));
                    }
                }

                void onDone(size_t numRows) {
                    std::cout << numRows << " commits loaded" << std::endl;
                }

            private:
                Verifier & v_;
            };

            class CommitNetworkLoader : public helpers::CSVReader {
            public:
                CommitNetworkLoader(Verifier & v, std::string const & filename):
                    v_(v) {
                    for (auto i : v_.projects_)
                        unaccountedProjects_.insert(i.first);
                    for (auto i : v_.commits_)
                        unaccountedCommits_.insert(i.first);
                    
                    std::cout << "Loading commit networks..." << std::endl;
                    parse(filename, true);
                    std::cout << numRows() << " lines read" << std::endl;
                    std::cout << unaccountedProjects_.size() << " unaccounted projects" << std::endl;
                    std::cout << unaccountedCommits_.size() << " unaccounted commits " << std::endl;
                    std::cout << skippedProjects_.size() << " skipped projects " << std::endl;
                    for (unsigned id : unaccountedProjects_) {
                        auto i = v_.projects_.find(id);
                        assert(i != v_.projects_.end());
                        delete i->second;
                        v_.projects_.erase(i);
                    }
                    for (unsigned id : unaccountedCommits_) {
                        auto i = v_.commits_.find(id);
                        assert(i != v_.commits_.end());
                        delete i->second;
                        v_.commits_.erase(i);
                    }
                }

            protected:
                
                void row(std::vector<std::string> & row) override {
                    unsigned pid = std::stoul(row[0]);
                    // empty project
                    if (row[1] == "NA")
                        return;
                    // otherwise get the project and commit
                    ProjectRecord * pr = v_.getProject(pid);
                    // if we are repeating, it is ok to have missing commits
                    if (pr == nullptr) {
                        skippedProjects_.insert(pid);
                        return;
                    }
                    CommitRecord * cr = v_.getCommit(row[1]);
                    auto i = unaccountedProjects_.find(pr->id);
                    if (i != unaccountedProjects_.end())
                        unaccountedProjects_.erase(i);
                    i = unaccountedCommits_.find(cr->id);
                    if (i != unaccountedCommits_.end())
                        unaccountedCommits_.erase(i);
                    pr->commits_.insert(cr);
                }

            private:
                std::unordered_set<unsigned> unaccountedProjects_;
                std::unordered_set<unsigned> unaccountedCommits_;
                std::unordered_set<unsigned> skippedProjects_;
                Verifier & v_;
            }; 

            /**
             */
            class CommitHistoryLoader : public helpers::CSVReader {
            public:
                CommitHistoryLoader(Verifier & v, std::string const & filename):
                    CSVReader('"',' '),
                    p_(nullptr),
                    v_(v) {
                    missingCommitDates_ = 0;
                    std::cout << "Loading commit history info..." << std::endl;
                    parse(filename, false);
                    std::cout << numRows() << " lines read" << std::endl;
                }

            protected:
                void row(std::vector<std::string> & row) override {
                    if (row[0] == "#") {
                        p_ = v_.getProject(std::stoul(row[1]));
                    } else if (p_ != nullptr) {
                        CommitRecord * cr = v_.getCommit(row[0]);
                        // ignore commits that we do not have in store
                        if (cr != nullptr) {
                            if (! row[1].empty() && ! row[2].empty()) {
                                cr->authorTime = std::stoull(row[1]);
                                cr->commitTime = std::stoull(row[2]);
                            } else {
                                ++missingCommitDates_;
                                std::cout << "Missing commit and author time info for: " << std::endl;
                                std::cout << p_->commitUrl(cr) << std::endl;
                            }
                        }
                    }
                }

            private:
                size_t missingCommitDates_;
                ProjectRecord * p_;
                Verifier & v_;
            };

            class ChangesLoader : public FileChange::Reader {
            public:
                ChangesLoader(Verifier & v, std::string const & filename, std::string const & output):
                    v_(v),
                    o_(output) {
                    std::cout << "Reading changes hashes..." << std::endl;
                    changesRetained_ = 0;
                    o_ << "#projectId,pathId,fileHashId,commitId" << std::endl;
                    readFile(filename);
                }

            protected:
                void onRow(unsigned projectId, unsigned pathId, unsigned fileHashId, unsigned commitId) override {
                    ProjectRecord * pr = v_.getProject(projectId);
                    // ignore changes from commits we have ignored
                    if (pr != nullptr) {
                        // get the commit info
                        CommitRecord * cr = v_.getCommit(commitId);
                        if (cr == nullptr) {
                            ++pr->invalidChanges_;
                            affectedProjects_.insert(projectId);
                            std::cout << "Invalid change: " << pr->user << "/" << pr->repo << "  -- commit id " << commitId << std::endl;
                        } else {
                            ++changesRetained_;
                            ++pr->validChanges_;
                            // delete the commit from project listed commits
                            auto i = pr->commits_.find(cr);
                            if (i != pr->commits_.end())
                                pr->commits_.erase(i);
                            // mark the path and hash
                            v_.paths_.insert(pathId);
                            v_.fileHashes_.insert(fileHashId);
                            o_ << projectId << "," << pathId << "," << fileHashId<< "," << commitId << std::endl;
                        }
                    }
                }

                void onDone(size_t numRows) {
                    std::cout << numRows << " changes analyzed" << std::endl;
                    std::cout << changesRetained_ << " changes retained" << std::endl;
                    std::cout << affectedProjects_.size() << " projects with invalid changes" << std::endl;
                    std::cout << v_.paths_.size() << " valid paths detected" << std::endl;
                    std::cout << v_.fileHashes_.size() << " valid file hashes detected " << std::endl;
                    // make sure that all valid project's commits were observed
                    size_t nonEmpty = 0;
                    for (auto i : v_.projects_)
                        if (!i.second->commits_.empty()) {
                            std::cout << "Project " << i.second->id << ": " << std::endl;
                            for (auto j : i.second->commits_)
                                std::cout << i.second->commitUrl(j) << "   -- " << j->id << std::endl;
                            ++nonEmpty;
                        }
                    std::cout << nonEmpty << " projects with unobserved commits" << std::endl;
                    assert(nonEmpty == 0);
                    std::cout << "Projects with commits I haven't seen yet" << std::endl;
                    bool needToRedo = false;
                    for (auto i : affectedProjects_) {
                        ProjectRecord * pr = v_.getProject(i);
                        std::cout << i << ": " << pr->invalidChanges_ << ", valid " << pr->validChanges_ << std::endl;
                        if (pr->validChanges_ != 0)
                            needToRedo = true;
                        delete pr;
                        auto j = v_.projects_.find(i);
                        v_.projects_.erase(j);
                    }
                    if (needToRedo) {
                        std::cout << "redoing because projects with invalid changes..." << std::endl;
                        throw RedoException();
                    }
                }

            private:
                size_t changesRetained_;
                size_t numInvalidChanges_;
                std::unordered_set<unsigned> invalidCommits_;
                std::unordered_set<unsigned> affectedProjects_;
                Verifier & v_;
                std::ofstream o_;
            };

            class PathsFilter : public Path::Reader {
            public:
                PathsFilter(Verifier & v, std::string const & filename, std::string const & output):
                    v_(v),
                    o_(output) {
                    std::cout << "Filtering paths..." << std::endl;
                    numPaths_ = 0;
                    o_ << "#id,path" << std::endl;
                    readFile(filename);
                }
            protected:
                void onRow(unsigned id, std::string const & path) override {
                    if (v_.paths_.find(id) != v_.paths_.end()) {
                        ++numPaths_;
                        o_ << id << "," << helpers::escapeQuotes(path) << std::endl;
                    }
                }

                void onDone(size_t numRows) override {
                    std::cout << numRows << " paths read" << std::endl;
                    std::cout << numPaths_ << " paths retained" << std::endl;
                }

            private:
                Verifier & v_;
                std::ofstream o_;
                size_t numPaths_;
                
            };

            class FileHashFilter : public FileHash::Reader {
            public:
                FileHashFilter(Verifier & v, std::string const & filename, std::string const & output):
                    v_(v),
                    o_(output) {
                    std::cout << "Filtering file hashes..." << std::endl;
                    numHashes_ = 0;
                    o_ << "#id,hash" << std::endl;
                    readFile(filename, false);
                }
            protected:
                void onRow(unsigned id, std::string const & path) override {
                    if (v_.fileHashes_.find(id) != v_.fileHashes_.end()) {
                        ++numHashes_;
                        o_ << id << "," << path << std::endl;
                    }
                }

                void onDone(size_t numRows) override {
                    std::cout << numRows << " hashes read" << std::endl;
                    std::cout << numHashes_ << " hashes retained" << std::endl;
                }

            private:
                Verifier & v_;
                std::ofstream o_;
                size_t numHashes_;
            };


            class CommitRecord {
            public:
                // id of the commit
                unsigned id;
                // hash of the commit
                std::string hash;
                // author time
                unsigned authorTime;
                // commit time
                unsigned commitTime;
                // commit parents
                std::vector<unsigned> parents;

                CommitRecord(unsigned id):
                    id(id),
                    authorTime(0),
                    commitTime(0) {
                }
            };

            class ProjectRecord {
            public:
                unsigned id;
                std::string user;
                std::string repo;
                uint64_t createdAt;
                int fork;
                unsigned committers;
                unsigned authors;
                unsigned watchers;
                ProjectRecord(unsigned id):
                    id(id),
                    createdAt(0),
                    fork(-1),
                    committers(0),
                    authors(0),
                    watchers(0),
                    validChanges_(0),
                    invalidChanges_(0) {
                }


                std::string commitUrl(CommitRecord * c) {
                    return STR("http://github.com/" << user << "/" << repo << "/commit/" << c->hash);
                }

                // we put project's commits in the set and then remove them when we see them in changes recorded. This way we can expect at the end of changes reading for each valid project the commits_ set to be empty. 
                std::unordered_set<CommitRecord *> commits_;


                unsigned validChanges_;
                unsigned invalidChanges_;

                
            };






            CommitRecord * getOrCreateCommit(unsigned id) {
                auto i = commits_.find(id);
                if (i == commits_.end())
                    i = commits_.insert(std::make_pair(id, new CommitRecord(id))).first;
                return i->second;
            }

            CommitRecord * getCommit(unsigned id) {
                auto i = commits_.find(id);
                if (i == commits_.end())
                    return nullptr;
                return i->second;
            }

            CommitRecord * getCommit(std::string const & hash) {
                auto i = hashToCommit_.find(hash);
                if (i == hashToCommit_.end())
                    return nullptr;
                return i->second;
            }

            ProjectRecord * getOrCreateProject(unsigned id) {
                auto i = projects_.find(id);
                if (i == projects_.end())
                    i = projects_.insert(std::make_pair(id, new ProjectRecord(id))).first;
                return i->second;
            }

            ProjectRecord * getProject(unsigned id) {
                auto i = projects_.find(id);
                if (i == projects_.end())
                    return nullptr;
                return i->second;
            }


            void writeCommits(std::string const & filename) {
                std::ofstream o(filename);
                o << "#id,hash,authorTime,commitTime" << std::endl;
                for (auto i : commits_) {
                    o << i.second->id << "," << helpers::escapeQuotes(i.second->hash) << "," << i.second->authorTime << "," <<  i.second->commitTime << std::endl;
                }
            }

            void writeProjects(std::string const & filename) {
                std::ofstream o(filename);
                o << "#id,user,repo,createdAt,forkOf,committers,authors,watchers" << std::endl;
                for (auto i : projects_) {
                    o << i.second->id << "," << helpers::escapeQuotes(i.second->user) << "," << helpers::escapeQuotes(i.second->repo) << "," << i.second->createdAt << "," << i.second->fork << "," << i.second->committers << "," << i.second->authors << "," << i.second->watchers << std::endl;
                }
            }

            
            
            
            std::unordered_map<unsigned, CommitRecord *> commits_;
            std::unordered_map<std::string, CommitRecord *> hashToCommit_;

            std::unordered_map<unsigned, ProjectRecord *> projects_;
            std::unordered_set<unsigned> paths_;
            std::unordered_set<unsigned> fileHashes_;

            
        }; // anonymous::Verifier
        
    } // anonymous namespace


    /** Sifts through the multitude of files and formats the data gathering and cleaning phases produce to create a single folder with simple data layout for further analyses.
     */
    void FilterVerifier(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        // first we load the commits hierarchies as reported by compact-commit-trees
        Verifier v;
        v.verify();

        
    }

    
} // namespace dejavu
