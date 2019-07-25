#include <algorithm>

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

            uint64_t iso8601ToTime(std::string const & str) {
                struct tm t;
                strptime(str.c_str(),"%Y-%m-%dT%H:%M:%SZ",&t);
                return mktime(&t);
            }

            
            void patchFromGithubMetadata() {
                if (!Input.isSpecified())
                    return;
                std::cerr << "Patching from downloaded metadata..." << std::endl;
                size_t errors = 0;
                size_t patched = 0;
                size_t paths = 0;
                size_t repatched = 0;
                size_t total = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    if (total++ % 1000 == 0)
                        std::cerr << "    " << (total/1000) << "k    \r" << std::flush;
                    std::string path = STR(Input.value() << "/" << (p->id % 1000) << "/" << p->id);
                    if (helpers::FileExists(path)) {
                        ++paths;
                        try {
                            nlohmann::json json;
                            std::ifstream (path) >> json;
                            std::string fullName = json["full_name"];
                            std::transform(fullName.begin(), fullName.end(), fullName.begin(),[](unsigned char c){ return std::tolower(c); });                            
                            if (fullName != p->user + "/" + p->repo) {
                                std::cout << fullName << " - " << p->user << "/" << p->repo << std::endl;
                                ++errors;
                                continue;
                            }
                            p->createdAt = iso8601ToTime(json["created_at"]);
                            if (p->patched)
                                ++repatched;
                            p->patched = true;
                            ++patched;
                        } catch (...) {
                            ++errors;
                        }
                    } 
                }
                std::cerr << "    " << paths << " downloaded metadata analyzed" << std::endl;
                std::cerr << "    " << patched << " patched or repatched projects" << std::endl;
                std::cerr << "    " << repatched << " repatched projects " << std::endl;
                std::cerr << "    " << errors << " errors" << std::endl;
            }

            void output() {
                std::ofstream f(DataDir.value() + "/projects.csv");
                std::ofstream pp(DataDir.value() + "/patchedProjects.csv");
                std::ofstream up(DataDir.value() + "/unpatchedProjects.csv");
                f << "projectId,user,repo,createdAt" << std::endl;
                pp << "projectId" << std::endl;
                up << "projectId,user,repo,createdAt" << std::endl;
                size_t patched = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    if (p->patched) {
                        ++patched;
                        pp << p->id << std::endl;
                    } else {
                        up << p->id << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
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

