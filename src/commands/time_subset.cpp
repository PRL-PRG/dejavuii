#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <string>

#include "helpers/strings.h"

#include "../commands.h"
#include "../loaders.h"
#include "../commit_iterator.h"

namespace dejavu {
#ifdef HAHA
    namespace {


        class Commit {
        public:
            bool taint;
            size_t id;
            uint64_t time;
            std::unordered_set<Commit *> parents;
            Commit(uint64_t time):
                taint(false),
                id(0),
                time(time) {
            }
            /*
            void taint(std::unordered_set<Commit *> & visited) {
                std::vector<Commit *> q;
                q.push_back(this);
                while (! q.empty()) {
                    //                    Commit * c = 
                }
                } */
        };

        class Change {
        public:
            unsigned project;
            unsigned path;
            unsigned contents;
            Commit * commit;

            class TimeComparator {
            public:
                bool operator () (Change * a, Change * b) const {
                    if (a->commit->time == b->commit->time)
                        return a < b;
                    else
                        return a->commit->time < b->commit->time;
                }
                
            };
        };



        /** Filters the datasetto only selected % of changes from the beginning.

            This is done by getting the commits and then the changes, ordering them using the commits. Then we select the appropriate cutoff for changes in terms of %.

            Next step is to ensure that we have all the data we need, i.e. mark commits in the changes we have used, and make sure that all changes from given commits are included as well as all changes from their parent commits.

            NODE: This may create a situation where a project has two "branches" alive at the end, but that should not pose any problems to the algorithms we use. 
         */
        class Filter {
        public:
            void filter() {
                // first load all commits
                std::cerr << "Loading commits..." << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime) {
                        commits_[id] = new Commit(authorTime);
                    }};
                std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;

                // then load all parents
                std::cerr << "Loading commit parents... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        assert(c != nullptr);
                        Commit * p = commits_[parentId];
                        assert(p != nullptr);
                        c->parents.insert(p);
                    }};

                // now load all changes
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        changes_.insert(new Change(projectId, pathId, contentsId_, commits_[commitId]));
                    }};
                std::cerr << "    " << changes_.size() << " loaded" << std::endl;
                size_t cutoff = changes_.size() * 100 / Pct.value();
                std::cerr << "Cutoff estimated at " << cutoff << " changes (" << Pct.value() << "%)" << std::endl;
                std::cerr << "Tainting changes ..." << std::endl;
                auto i = changes_.begin();
                for (; cutoff > 0; ++i) {
                    i->commit->taint = true;
                    --cutoff;
                }
                std::cerr << "Tainting commits..." << std::endl;
                {
                    std::unordered_set<Commit *> visited;
                    for (auto i : commits_) {
                        Commit * c = i.second;
                        c->taint(visited);
                    }
                }
                
                
            }




            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_set<Change *, Change::TimeComparator> changes_;

            
        }; 


    } // anonymous namespace
#endif        
    


    void TimeSubset(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.addOption(Pct);
        Settings.parse(argc, argv);
        Settings.check(); 

        //  Filter f;
        //f.filter();
        
    }
    
} // nameaspace dejavu
