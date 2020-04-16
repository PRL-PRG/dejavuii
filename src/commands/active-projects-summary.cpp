#include <unordered_set>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;
            
            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt) {
            }

            void addChange(unsigned commitId, uint64_t time) {
                ++changes;
                if (commits.insert(commitId).second) {
                    if (time > youngestCommit)
                        youngestCommit = time;
                    if (time < oldestCommit)
                        oldestCommit = time;
                }
            }

            std::unordered_set<unsigned> commits;

            uint64_t oldestCommit = std::numeric_limits<uint64_t>::max();
            uint64_t youngestCommit = 0;
            
            size_t changes = 0;
            
        };

        class Worker {
        public:
            void loadData() {
                std::cerr << "Loading projects..." << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                // we need to know commit times
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commitTimes_.insert(std::make_pair(id, authorTime));
                    }};
                std::cerr << "    " << commitTimes_.size() << " commits loaded" << std::endl;
            }

            // iterates over the file changes and updates the project counts
            void summarize() {
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        p->addChange(commitId, commitTimes_[commitId]);
                    }};
            }

            void output() {
                std::cerr << "Writing results..." << std::endl;
                std::ofstream f(DataDir.value() + "/activeProjectsSummary.csv");
                f << "projectId,createdAt,numCommits,oldest,youngest,numChanges" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->createdAt << "," << p->commits.size() << "," << p->oldestCommit << "," << p->youngestCommit << "," << p->changes << std::endl;
                }
            }
                

        private:

            std::unordered_map<unsigned, Project *> projects_; 
            std::unordered_map<unsigned, uint64_t> commitTimes_;
            
        }; 
        
    }
    void ActiveProjectsSummary(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();


        Worker w;
        w.loadData();
        w.summarize();
        w.output();
    }
    
}
