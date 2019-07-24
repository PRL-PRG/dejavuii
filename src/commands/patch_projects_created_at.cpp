#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"

#include "helpers/json.hpp"

namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            std::string user;
            std::string repo;
            uint64_t createdAt;
            bool patched;
        };

        class Patcher {
        public:
            
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo, createdAt, false};
                        projects_.insert(std::make_pair(id, p));
                        projectsByName_.insert(std::make_pair(user + "/" + repo, p));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                {
                    size_t patched = 0;
                    std::cerr << "Loading patch status..." << std::endl;
                    std::string filename = DataDir.value() + "/patchedProjects.csv";
                    if (helpers::FileExists(filename)) {
                        IdLoader{filename, [&, this](unsigned projectId) {
                                auto i = projects_.find(projectId);
                                assert(i != projects_.end());
                                assert(i->second->patched == false);
                                i->second->patched = true;
                                ++patched;
                            }};
                    }
                    std::cerr << "    " << patched << " already patched projects" << std::endl;
                }
            }

            void patchFromGhTorrent() {
                if (!GhtDir.isSpecified())
                    return;
                std::cerr << "Patching from ghtorrent data..." << std::endl;
                size_t total = 0;
                size_t patched = 0;
                GHTorrentProjectsLoader(GhtDir.value() + "/projects.csv", [&,this](unsigned id,
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
                        auto i = projectsByName_.find(index);
                        if (i == projectsByName_.end())
                            return;
                        Project * p = i->second;
                        // now update createdAt
                        ++patched;
                        if (p->patched) {
                            if (p->createdAt < createdAt)
                                p->createdAt = createdAt;
                        } else {
                            p->createdAt = createdAt;
                            p->patched = true;
                        }
                    });
                std::cerr << "    " << total << " projects in GHTorrent" << std::endl;
                std::cerr << "    " << patched << " newly patched projects" << std::endl;
                
            }

            void patchFromGithubMetadata() {
                for (auto i : projects_) {
                    Project * p = i.second;
                    std::string path = STR(Input.value() << "/" << (p->id % 1000) << "/" << p->id);
                    if (helpers::FileExists(path)) {
                        nlohmann::json json;
                        std::ifstream (path) >> json;
                        std:: cout << json["created-at"] << std::endl;
                        exit(-1);
                    } 
                }
                
            }

            void output() {
                std::ofstream f(DataDir.value() + "/projects.csv");
                std::ofstream pp(DataDir.value() + "/patchedProjects.csv");
                std::ofstream up(DataDir.value() + "/unpatchedProjects.csv");
                f << "projectId,user,repo,createdAt" << std::endl;
                pp << "projectId" << std::endl;
                up << "projectId" << std::endl;
                size_t patched = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    if (p->patched) {
                        ++patched;
                        pp << p->id << std::endl;
                    } else {
                        up << p->id << std::endl;
                    }
                }
                std::cout << "    " << patched << " patched projects after the stage" << std::endl;


                
            }

        private:
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<std::string, Project *> projectsByName_;


            
        }; 

    } // anonymous namespace


    void PatchProjectsCreatedAt(int argc, char * argv[]) {
        GhtDir.required = false;
        Input.required = false;
        Settings.addOption(DataDir);
        Settings.addOption(Input);
        Settings.addOption(GhtDir);
        Settings.parse(argc, argv);
        Settings.check();

        Patcher p;
        p.loadData();
        p.patchFromGhTorrent();
        p.patchFromGithubMetadata();
        p.output();
        
    }
    
} // namespace dejavu

#ifdef HAHA


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
                            p->updated = true;
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
                {
                    std::cerr << "Writing projects..." << std::endl;
                    std::ofstream f(DataDir.value() + "/projectsFixed.csv");
                    f << "#projectId,user,repo,createdAt" << std::endl;
                    for (auto i : projects_)
                        f << *(i.second) << std::endl;
                }
                {
                    std::cerr << "Writing projects not found in ghtorrent..." << std::endl;
                    size_t notFound = 0;
                    std::ofstream f(DataDir.value() + "/projectsNotFoundInGHT.csv");
                    f << "projectId,user,repo" << std::endl;
                    for (auto i : projects_) {
                        Project * p = i.second;
                        if (! p->updated) {
                            ++notFound;
                            f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << std::endl;
                        }
                    }
                    std::cout << "    " << notFound << " projects not found in GHT" << std::endl;
                }
                std::cerr << "Done." << std::endl;
            }

        private:
            
            std::unordered_map<std::string, Project *> projects_;
            std::unordered_map<unsigned, Project *> ghtProjects_;
            
        }; // Patcher
        
    }

    void PatchProjectsCreatedAt(int argc, char * argv []) {
        Settings.addOption(DataDir);
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


#endif
