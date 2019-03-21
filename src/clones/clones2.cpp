#include <cassert>
#include <set>
#include <vector>

#include "helpers/csv-reader.h"
#include "../settings.h"

#include "clones2.h"


namespace dejavu {
    namespace {

        helpers::Option<std::string> InputDir("inputDir", "/verified", false);

        
        

        class FolderCloneDetector {
        public:
            FolderCloneDetector() {
            
            }

            void loadData() {
                ProjectRecord::Loader(projects_, DataRoot.value() + InputDir.value() + "/projects.csv");
                CommitRecord::Loader(commits_, DataRoot.value() + InputDir.value() + "/commits.csv");
                CommitRecord::NetworkLoader(commits_, DataRoot.value() + InputDir.value() + "/compacted-commits.csv");
            }
            
        private:


            /** Record for a single commit.

                Contains the id of the commit, the earliest time the commit was created (the minimum of commit and author times), the index of the commit in global topological order, set of changes the commit makes to files (pathId to fileHashId) and vector of the commits children in the commits graph. 
            */
            class CommitRecord {
            public:
                // id of the commit
                unsigned id;
                // minimum from the author & commit times
                uint64_t time;
                // index of the commit in global topological ordering
                unsigned topologicalIndex;
                // list of changes for the commit
                std::unordered_map<unsigned, unsigned> changes;
                // direct children of the commit in the global commits graph
                std::vector<unsigned> children;
                // number of parents (determines whether the commit is a merge)
                unsigned numParents;

                CommitRecord(unsigned id, uint64_t time):
                    id(id),
                    time(time),
                    topologicalIndex(0),
                    numParents(0) {
                }

                /** Comparator for commit records based on their topological ordering.
                 */
                struct TopologicalComparator {
                    bool operator()(CommitRecord * first, CommitRecord * second) {
                        assert(first->topologicalIndex != second->topologicalIndex);
                        return first->topologicalIndex < second->topologicalIndex;
                    }
                
                }; // CommitRecord::TopologicalComparator

                class Loader : public helpers::CSVReader {
                public:
                    Loader(std::vector<CommitRecord *> & into, std::string const & filename):
                        into_(into) {
                        std::cout << "Loading commits from " << filename << std::endl;
                        parse(filename, true);
                        std::cout << "    " << numRows() << " commits loaded (container size " << into.size() << ")" << std::endl;
                        std::cout << "    compacting..." << std::endl;
                        into.shrink_to_fit();
                        std::cout << "    done." << std::endl;
                    }
                protected:
                    void row(std::vector<std::string> & row) override {
                        unsigned id = std::stoul(row[0]);
                        uint64_t aTime = std::stoull(row[2]);
                        uint64_t cTime = std::stoull(row[3]);
                        if (id >= into_.size())
                            into_.resize(id + 1);
                        into_[id] = new CommitRecord(id, aTime < cTime ? aTime: cTime);
                    }

                private:
                    std::vector<CommitRecord *> & into_;
                };

                class NetworkLoader : public helpers::CSVReader {
                public:
                    NetworkLoader(std::vector<CommitRecord *> & into, std::string const & filename):
                        commits_(into),
                        ignored_(0),
                        maxChildren_(0),
                        maxParents_(0),
                        splits_(0),
                        joins_(0) {
                        std::cout << "Loading commit network from " << filename << std::endl;
                        parse(filename, true);
                        std::cout << "    " << ignored_ << " ignored commits" << std::endl;
                        std::cout << "    " << splits_ << " commits with more than 1 child" << std::endl;
                        std::cout << "    " << joins_ << "  commits with more than 1 parent" << std::endl;
                        std::cout << "    " << maxChildren_ << " max number of children per commit" << std::endl;
                        std::cout << "    " << maxParents_ << " max number of parents per commit" << std::endl;
                    }
                    
                protected:
                    // commitId, parentId
                    void row(std::vector<std::string> & row) override {
                        unsigned id = std::stoul(row[0]);
                        if (id >= commits_.size() || commits_[id] == nullptr) {
                            ++ignored_;
                            return;
                        }
                        if (row[1] == "NA") 
                            return;
                        unsigned parentId = std::stoul(row[1]);
                        CommitRecord * c = commits_[id];
                        CommitRecord * p = commits_[parentId];
                        assert(c != nullptr);
                        if (p == nullptr) {
                            //                            ++ignored_;
                            std::cout << parentId << std::endl;
                            return;
                        }
                        assert(p != nullptr);
                        unsigned x = ++c->numParents;
                        if (x > maxParents_)
                            maxParents_ = x;
                        if ( x== 725)
                            std::cout << "max parents: " << c->id << std::endl;
                        p->children.push_back(c->id);
                        if (p->children.size() > maxChildren_)
                            maxChildren_ = p->children.size();
                        if (x == 2)
                            ++joins_;
                        if (p->children.size() == 2)
                            ++splits_;
                    }

                private:
                    std::vector<CommitRecord *> & commits_;
                    unsigned ignored_;
                    unsigned maxChildren_;
                    unsigned maxParents_;
                    unsigned splits_;
                    unsigned joins_;
                    
                };
            
            };
            
            /** Project information optimized for the folder clone detection.
             */
            class ProjectRecord {
            public:
                // id of the project
                unsigned id;
                // time at which the project was created
                uint64_t createdAt;
                // set of the commits of the project in topological order
                std::set<CommitRecord *, CommitRecord::TopologicalComparator> commits;

                ProjectRecord(unsigned id, uint64_t createdAt):
                    id(id),
                    createdAt(createdAt) {
                }

                class Loader : public helpers::CSVReader {
                public:
                    Loader(std::vector<ProjectRecord *> & into, std::string const & filename):
                        into_(into) {
                        std::cout << "Loading projects from " << filename << std::endl;
                        parse(filename, true);
                        std::cout << "    " << numRows() << " projects loaded (container size " << into.size() << ")" << std::endl;
                        std::cout << "    compacting..." << std::endl;
                        into.shrink_to_fit();
                        std::cout << "    done." << std::endl;
                    }
                protected:
                    void row(std::vector<std::string> & row) override {
                        unsigned id = std::stoul(row[0]);
                        uint64_t createdAt = std::stoull(row[3]);
                        if (id >= into_.size())
                            into_.resize(id + 1);
                        into_[id] = new ProjectRecord(id, createdAt);
                    }

                private:
                    std::vector<ProjectRecord *> & into_;
                };
            };


        private:
            std::vector<ProjectRecord *> projects_;
            std::vector<CommitRecord *> commits_;
        
        }; // FolderCloneDetector


    } // anonymous namespace


    void FolderClones(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.parse(argc, argv);
        settings.check();
        FolderCloneDetector fcd;
        fcd.loadData();
        
    }
    
    
} // namespace dejavu 
