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
#include <fstream>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Project {
        public:
            uint64_t oldestCommit = std::numeric_limits<uint64_t>::max();
            uint64_t youngestCommit = std::numeric_limits<uint64_t>::min();

            void updateWithCommitTime(uint64_t time) {
                if (time < oldestCommit)
                    oldestCommit = time;
                if (time > youngestCommit)
                    youngestCommit = time;
            }
        };

        class LifespanCalculator {
        public:
            void loadData() {
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, authorTime));
                    }};
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        projects_[projectId].updateWithCommitTime(commits_[commitId]);
                    }};
            }

            void output() {
                std::cout << "Writing projects..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectLifespans.csv");
                f << "projectId,oldestCommitTime,youngestCommitTime" << std::endl;
                for (auto i : projects_) 
                    f << i.first << "," << i.second.oldestCommit << "," << i.second.youngestCommit << std::endl;
                std::cout << "    " << projects_.size() << " projects written" << std::endl;
            }


        private:
            std::unordered_map<unsigned, uint64_t> commits_;
            std::unordered_map<unsigned, Project> projects_;
            
        }; // LifespanCalculator
        
    } // anonymous namespace



    void ProjectLifespan(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        LifespanCalculator lc;
        lc.loadData();
        lc.output();

        
    }
} // namespace dejavu
