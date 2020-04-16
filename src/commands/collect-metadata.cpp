#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"

#include "helpers/json.hpp"
#include "helpers/strings.h"


namespace dejavu {

    namespace {

        class Collector {
        public:

            void collect() {
                size_t projects = 0;
                size_t notFound = 0;
                size_t patched = 0;
                std::ofstream f{DataDir.value() + "/projectsMetadata.csv"};
                f << "projectId,watchers,stars,forks,openIssues,hasDownloads,hasWiki" << std::endl;
                std::cerr << "Collecting projects ... " << std::endl;
                ProjectLoader{[&, this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        ++projects;
                        std::string path = helpers::ToLower(STR(user << "_" << repo));
                        path = Input.value() + "/" + path.substr(0, 2) + "/" + path + ".json";
                        if (helpers::FileExists(path)) {
                            try {
                                nlohmann::json json;
                                std::ifstream{path} >> json;
                                size_t watchers = json["watchers_count"];
                                size_t stars = json["stargazers_count"];
                                size_t forks = json["forks_count"];
                                size_t openIssues = json["open_issues"];
                                bool hasDownloads = json["has_downloads"];
                                bool hasWiki = json["has_wiki"];
                                f << id << "," << watchers << "," << stars << "," << forks << "," << openIssues << "," << (hasDownloads ? 1 : 0) << "," << (hasWiki ? 1 : 0) << std::endl;
                                ++patched;
                            } catch (std::exception const & e) {
                                std::cout << "error: " << e.what() << std::endl;
                            }
                        } else {
                            ++notFound;
                        }
                }};
                std::cerr << "    " << projects << " projects loaded" << std::endl;
                std::cerr << "    " << patched << " projects patched" << std::endl;
                std::cerr << "    " << notFound << " metadata not found" << std::endl;
            }
        };
    }

    void CollectMetadata(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(Input);

        Settings.parse(argc, argv);
        Settings.check();

        Collector c;
        c.collect();
        
    }
}
