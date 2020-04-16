#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            std::string user;
            std::string repo;
            uint64_t createdAt;

            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                id{id},
                user{user},
                repo{repo},
                createdAt{createdAt} {
            }
        };


        class AgeFilter {
        public:
            
            void loadData() {
                std::cerr << "Loading projects..." << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, user, repo, createdAt)));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        if (authorTime < 1199145600) // 1.1.2018
                            tooOldCommits_.insert(id);
                        else if (authorTime >= 1546300800) //1.1.2019
                            tooNewCommits_.insert(id);
                    }};
                std::cerr << "    " << tooOldCommits_.size() << " commits too old" << std::endl;
                std::cerr << "    " << tooNewCommits_.size() << " commits too new" << std::endl;
            }

            void filterProjects() {
                std::cerr << "filtering projects..." << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        if (tooOldCommits_.find(commitId) != tooOldCommits_.end()) {
                            auto i = projects_.find(projectId);
                            if (i != projects_.end()) {
                                delete i->second;
                                projects_.erase(i);
                            }
                        }
                    }};
                std::cerr << "    " << projects_.size() << " valid projects left" << std::endl;
                std::ofstream f{OutputDir.value() + "/projects.csv"};
                f << "projectid,user,repo,createdAt" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << ","
                      << helpers::escapeQuotes(p->user) << ","
                      << helpers::escapeQuotes(p->repo) << ","
                      << p->createdAt << std::endl;
                }
            }

            void filter() {
                std::unordered_set<unsigned> validCommits;
                {
                    std::cerr << "Filtering file changes..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/fileChanges.csv");
                    f << "projectId,commitId,pathId,contentsId" << std::endl;
                    size_t total = 0;
                    size_t valid = 0;
                    FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++total;
                            if (projects_.find(projectId) != projects_.end() && tooNewCommits_.find(commitId) == tooNewCommits_.end()) {
                                ++valid;
                                validCommits.insert(commitId);
                                f << projectId << "," << commitId << "," << pathId << "," << contentsId << std::endl;
                            }
                        }};
                    std::cerr << "    " << total << " file changes observed" << std::endl;
                    std::cerr << "    " << valid << " file changes kept" << std::endl;
                    std::cerr << "    " << validCommits.size() << " valid commits detected" << std::endl;
                }
                {
                    std::cerr << "Filtering commits..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commits.csv");
                    f << "commitId,authorTime,committerTime" << std::endl;
                    size_t total = 0;
                    size_t valid = 0;
                    CommitLoader{[&,this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                            ++total;
                            if (validCommits.find(id) == validCommits.end())
                                return;
                            ++valid;
                            f << id << "," << authorTime << "," << committerTime << std::endl;
                        }};
                    std::cerr << "    " << total << " commits observed" << std::endl;
                    std::cerr << "    " << valid << " commits kept" << std::endl;
                }
                {
                    std::cerr << "Filtering commit parents..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commitParents.csv");
                    f << "commitId,parentId" << std::endl;
                    size_t total = 0;
                    size_t valid = 0;
                    CommitParentsLoader{[&, this](unsigned id, unsigned parentId){
                            ++total;
                            if (validCommits.find(id) == validCommits.end())
                                return;
                            if (validCommits.find(parentId) == validCommits.end())
                                return;
                            ++valid;
                            f << id << "," << parentId << std::endl;
                        }};
                    std::cerr << "    " << total << " links observed" << std::endl;
                    std::cerr << "    " << valid << " links kept" << std::endl;
                }
            }

            void createSymlinks() {
                std::cerr << "Creating symlinks..." << std::endl;
                helpers::System(STR("ln -s " << DataDir.value() + "/hashes.csv " << OutputDir.value() << "/hashes.csv"));
                helpers::System(STR("ln -s " << DataDir.value() + "/paths.csv " << OutputDir.value() << "/paths.csv"));
            }

        private:

            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_set<unsigned> tooOldCommits_;
            std::unordered_set<unsigned> tooNewCommits_;

            
        }; // Filter

        
    } // anonymous namespace


    /** 1) removes all projects that have commits before 2008
        2) removes all commits *after* 2018
     */
    void Filter2008to2018(int argc, char * argv []) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        helpers::EnsurePath(OutputDir.value());

        AgeFilter f;
        f.loadData();
        f.filterProjects();
        f.filter();
        f.createSymlinks();
                                       
        
    }
    
} // namespace dejavu
