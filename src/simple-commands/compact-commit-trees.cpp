#include <unordered_set>

#include "simple-commands.h"

#include "../settings.h"
#include "../objects.h"

namespace dejavu {

    namespace {
        
        helpers::Option<std::string> InputDir("inputDir", "/processed", false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", false);


        
        class CommitsLoader : public Commit::Reader {
        public:
            static unsigned IdOf(std::string const & hash) {
                auto i = hashToId_.find(hash);
                assert(i != hashToId_.end());
                return i->second;
            }
            
        protected:
            void onRow(unsigned id, std::string const & hash, uint64_t time) {
                hashToId_[hash] = id;
            }

            void onDone(size_t numRows) {
                std::cout << "Loaded " << hashToId_.size() << " commits" << std::endl;
            }
            
            static std::unordered_map<std::string, unsigned> hashToId_;
        };

        std::unordered_map<std::string, unsigned> CommitsLoader::hashToId_;

        class CommitsTreeCompactor : public helpers::CSVReader {
        public:
            void compact(std::string const & filename) {
                numRows_ = 0;
                numProjects_ = 0;
                numCommits_ = 0;
                numCompactions_ = 0;
                numEmptyProjects_ = 0;
                writtenEdges_ = 0;
                compacted_.open(DataRoot.value() + OutputDir.value() + "/compacted-commits.csv");
                parse(filename, true);
                std::cout << "Rows read:         " << numRows_ << std::endl;
                std::cout << "Projects:          " << numProjects_ << std::endl;
                std::cout << "Empty projects:    " << numEmptyProjects_ << std::endl;
                std::cout << "Commit edges:      " << numCommits_ << std::endl;
                std::cout << "Compacted edges:   " << numCompactions_ << std::endl;
                std::cout << "Visited commits:   " << commitsDone_.size() << std::endl;
                std::cout << "Written edges:     " << writtenEdges_ << std::endl;
            }

        protected:

            class Commit {
            public:
                Commit(unsigned id):
                    id(id) {
                }
                unsigned id;
                std::unordered_set<Commit *> parents;
                std::unordered_set<Commit *> children;

                bool isBranchCommit() const {
                    return children.size() >= 2;
                }

                bool isMergeCommit() const {
                    return parents.size() >= 2;
                }

                bool isDominatedBy(Commit * c) const {
                    std::unordered_set<Commit *> processed;
                    std::vector<Commit *> q;
                    for (auto i : children)
                        q.push_back(i);
                    while (!q.empty()) {
                        Commit * x = q.back();
                        q.pop_back();
                        if (processed.find(x) != processed.end())
                            continue;
                        processed.insert(x);
                        // if we found the dominator commit, all is good, try others
                        if (x == c)
                            continue;
                        // if we got to commit with no children without finding the dominator, tough luck...
                        if (x->children.empty())
                            return false;
                        // otherwise we must look in all children
                        for (auto i : x->children)
                            q.push_back(i);
                    }
                    return true;
                }
                    
            };


            void onProjectDone() {
                //                std::cout << "Done project " << activeProject_ << ", commits: " << commits_.size() << std::endl;
                if (commits_.empty())
                    ++numEmptyProjects_;
                // find all commits whose children are immediate merges
                for (auto i : commits_) {
                    // if the current commit branches
                    Commit * c = i.second;
                    if (c->isBranchCommit()) {
                        // and one of its children is a merge commit
                        for (auto child = c->children.begin(); child != c->children.end(); ) {
                            if ((*child)->isMergeCommit()) {
                                if (c->isDominatedBy(*child)) {
                                    (*child)->parents.erase(c);
                                    child = c->children.erase(child);
                                    ++numCompactions_;
                                    continue;
                                }
                            }
                            ++child;
                        }
                    }
                }
                // output the edges
                for (auto i : commits_) {
                    Commit * c = i.second;
                    if (commitsDone_.find(c->id) != commitsDone_.end())
                        continue;
                    commitsDone_.insert(c->id);
                    for (Commit * p : c->parents) {
                        compacted_ << c->id << "," << p->id << std::endl;
                        ++writtenEdges_;
                    }
                }
                for (auto i : commits_)
                    delete i.second;
                commits_.clear();
                ++numProjects_;
            }

            void row(std::vector<std::string> & row) override {
                unsigned pid = std::stoul(row[0]);
                if (pid != activeProject_)
                    onProjectDone();
                activeProject_ = pid;
                if (row[1] != "NA") {
                    Commit * commit = getOrCreateCommit(CommitsLoader::IdOf(row[1]));
                    Commit * parent = getOrCreateCommit(CommitsLoader::IdOf(row[2]));
                    commit->parents.insert(parent);
                    parent->children.insert(commit);
                    ++numCommits_;
                }
                ++numRows_;
            }

            Commit * getOrCreateCommit(unsigned id) {
                auto i = commits_.find(id);
                if (i == commits_.end())
                    i = commits_.insert(std::make_pair(id, new Commit(id))).first;
                return i->second;
            }

        private:
            size_t numRows_;
            size_t numProjects_;
            size_t numCommits_;
            size_t numCompactions_;
            size_t numEmptyProjects_;
            size_t writtenEdges_;
            unsigned activeProject_;
            std::ofstream compacted_;

            std::unordered_map<unsigned, Commit *> commits_;
            // we output edges per commit, not per project-commit pair
            std::unordered_set<unsigned> commitsDone_;
            
        };
            
        
        
    } // anonymous namespace 

    /** Compacts the commit trees.

        When commits that do not change javascript files are removed from the commit histories, a lot of branches are a bit empty. These will only add unnecessary complexity to the clone detection algorithm so we remove them.

        Furthermore, we drop the project ids which are redundant and convert the hashes of the commits to their ids. 
     */
    void CompactCommitTrees(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        {
            CommitsLoader cl;
            cl.readFile(DataRoot.value() + InputDir.value() + "/commits.csv", false);
        }
        {
            CommitsTreeCompactor ctc;
            ctc.compact(DataRoot.value() + InputDir.value() + "/selective-commit-network.csv");
            
        }
    }

    
} // namespace dejavu
