#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"


namespace dejavu {

    namespace {

        class Commit {
        };

        class Project : public BaseProject<Project, Commit> {
        public:

            std::string user;
            std::string repo;
            unsigned forkedFrom;
            bool updated;
            
            
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt),
                user(user),
                repo(repo),
                forkedFrom(NO_FORK),
                updated(false) {
            }

            std::string indexKey() {
                return user + "/" + repo;
            }

            friend std::ostream & operator << (std::ostream & s, Project const & p) {
                s << p.id << "," << helpers::escapeQuotes(p.user) << "," << helpers::escapeQuotes(p.repo) << "," << p.createdAt;
                return s;
            }
        };


        
        class Patcher {
        public:

            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project(id, user, repo, createdAt);
                        std::string index = p->indexKey();
                        assert(projects_.find(index) == projects_.end() && "Duplicate project");
                        projects_.insert(std::make_pair(index, p));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                std::cerr << "Loading GHTorrent projects..." << std::endl;
                size_t total = 0;
                size_t updates = 0;
                size_t updatedProjects = 0;
                GHTorrentProjectsLoader loader([&,this](unsigned id,
                                                      std::string const & url,
                                                      unsigned ownerId,
                                                      std::string const & name,
                                                      std::string const & description,
                                                      std::string const & language,
                                                      uint64_t createdAt,
                                                      unsigned forkedFrom,
                                                      uint64_t deleted,
                                                      uint64_t updatedAt){
                        // first get the user and repo strings from the url and check if we have the project in our database
                        ++total;
                        std::string index;
                        if (url.find("https://api.github.com/repos/") == 0) {
                            index = url.substr(29);
                        } else if (url.find("https://api./repos/") == 0) {
                            index = url.substr(19);
                        } else if (url == "\\N") {
                            return; // skip projects w/o urls
                        } else {
                            std::cerr << "Invalid url format: " << url << std::endl;
                            return;
                        }
                        auto i = projects_.find(index);
                        if (i == projects_.end())
                            return;
                        // now update createdAt
                        ++updates;
                        Project * p = i->second;
                        if (p->updated) {
                            if (p->createdAt < createdAt) {
                                p->createdAt = createdAt;
                                ghtProjects_[id] = p;
                            }
                        } else {
                            p->createdAt = createdAt;
                            ++updatedProjects;
                            ghtProjects_[id] = p;
                        }
                        // check whether the project is a fork (we have github id as a fork then)
                        if (forkedFrom != NO_FORK) {
                            p->forkedFrom = forkedFrom;
                        }
                        
                    });
                std::cerr << "    " << total << " projects in GHTorrent" << std::endl;
                std::cerr << "    " << updates << " project updates" << std::endl;
                std::cerr << "    " << updatedProjects << " updated projects" << std::endl;
            }

            void updateForks() {
                std::cerr << "Updating fork information..." << std::endl;
                unsigned unknownForks = 0;
                unsigned forks = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    if (p->forkedFrom != NO_FORK) {
                        auto j = ghtProjects_.find(p->forkedFrom);
                        if (j == ghtProjects_.end()) {
                            p->forkedFrom = UNKNOWN_FORK;
                            ++unknownForks;
                        } else {
                            p->forkedFrom = j->second->id;
                            ++forks;
                        }
                    }
                }
                std::cerr << "    " << unknownForks << " unknown forks" << std::endl;
                std::cerr << "    " << forks << " matched forks" << std::endl;

                // TODO we don't really output the fork anywhere
            }

            void output() {
                std::cerr << "Writing projects..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectsFixed.csv");
                f << "#projectId,user,repo,createdAt" << std::endl;
                for (auto i : projects_)
                    f << *(i.second) << std::endl;
                std::cerr << "Done." << std::endl;
            }

        private:
            
            std::unordered_map<std::string, Project *> projects_;
            std::unordered_map<unsigned, Project *> ghtProjects_;
            
        }; // Patcher
        
    }

    void PatchProjectsCreatedAt(int argc, char * argv []) {
        Settings.addOption(DataDir);
        Settings.addOption(GhtDir);
        Settings.parse(argc, argv);
        Settings.check();

        Patcher p;
        p.loadData();
        p.updateForks();
        p.output();
    }
    
} // namespace dejavu


