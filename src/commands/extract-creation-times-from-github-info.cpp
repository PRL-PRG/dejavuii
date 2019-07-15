#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>
#include <ctime>
#include <chrono>
#include <iomanip>

#include "../loaders.h"
#include "helpers/json.hpp"

namespace dejavu {

    namespace {

        class Stats {
        public:
            static size_t parsed;
            static size_t parsed_correctly;
            static size_t parsed_incorrectly;
        };
        size_t Stats::parsed = 0;
        size_t Stats::parsed_correctly = 0;
        size_t Stats::parsed_incorrectly = 0;

        struct Repository {
            std::string user;
            std::string project;
            std::string creation_date;

            uint64_t creation_date_as_epoch () {
//                std::stringstream s(creation_date);
//                std::tm t{};
//                s >> std::get_time(&t);
//                auto time_point = std::chrono::system_clock::from_time_t(std::mktime(&t));
//                return time_point.time_since_epoch().count();
                return 666;
            };

            static Repository FromJSON(std::string file) {
                try {
                    nlohmann::json data;

                    // Open and load the file into the JSON parser.
                    std::ifstream input(file);
                    assert(input.is_open());
                    input >> data;

                    Repository repository;
                    repository.project = data["name"];
                    repository.user = data["owner"]["login"];
                    repository.creation_date = data["created_at"];

                    ++Stats::parsed_correctly;
                    return repository;

                } catch (nlohmann::json::parse_error& e) {
                    ++Stats::parsed_incorrectly;
                    std::cerr << "could not parse: " << file
                              << " ("<< e.what() << ")" << std::endl;
                    Repository r;
                    return r;
                }
            }
        };

        struct RepositoryHash {
            size_t operator()(const Repository &r) const noexcept {
                return std::hash<std::string>()(r.user) << 32
                       | std::hash<std::string>()(r.project);
            }
        };

        struct RepositoryComp {
            bool operator()(const Repository &r1,
                            const Repository &r2) const noexcept {
                return r1.project == r2.project
                       && r1.user == r2.user
                       && r1.creation_date == r2.creation_date;
            }
        };

        void LoadRepositoriesFromJSON(
                std::unordered_set<Repository, RepositoryHash, RepositoryComp> &repositories) {
            clock_t timer = clock();
            size_t total_repositories = 0;
            std::string task = "loading repository info from JSON files ";
            helpers::StartTask(task, timer);



            helpers::FinishTask(task, timer);
        }
    };

    void ExtractCreationTimesFromGitHubInfo(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        //std::unordered_set<Repository, RepositoryHash, RepositoryComp> repositories;
        //LoadRepositoriesFromJSON(repositories);
//        SaveRepositories(repositories);
    }

};