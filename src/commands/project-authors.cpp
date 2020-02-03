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


namespace dejavu {

    namespace {

        class Project {
        public:
            Project(unsigned id):
                id{id},
                numCommits{0} {
            }
            
            unsigned id;
            std::unordered_set<unsigned> authors;
            std::unordered_set<unsigned> committers;
            size_t numCommits;
        };


        class ProjectDevelopers {
        public:
            void loadData() {
                // just get projects
                std::cerr << "Loading projects..." << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project{id}));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                std::cerr << "Loading commits and authors..." << std::endl;
                StringRowLoader{DataDir.value() + "/commitAuthors.csv", [this](std::vector<std::string> const & row){
                        unsigned commitId = std::stoul(row[0]);
                        unsigned authorId = std::stoul(row[1]);
                        unsigned committerId = std::stoul(row[2]);
                        commitAuthors_[commitId] = authorId;
                        commitCommitters_[commitId] = committerId;
                    }};
                std::cerr << "    " << commitAuthors_.size() << " commits loaded" << std::endl;
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        assert(commitAuthors_.find(commitId) != commitAuthors_.end());
                        p->authors.insert(commitAuthors_[commitId]);
                        p->committers.insert(commitCommitters_[commitId]);
                    }};
            }

            void output() {
                std::cerr << "Writing weekly activity details" << std::endl;
                std::ofstream f(DataDir.value() + "/projectAuthors.csv");
                f << "projectId,numAuthors,numCommitters" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->authors.size() << "," << p->committers.size() << std::endl;
                }
            }

        private:
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, unsigned>  commitAuthors_;
            std::unordered_map<unsigned, unsigned>  commitCommitters_;
            
        }; 

        
    } // anonymous namespace

    void ProjectAuthors(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        ProjectDevelopers p;
        p.loadData();
        p.output();
    }


    
} // namespace dejavu




