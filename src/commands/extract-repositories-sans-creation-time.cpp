#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>

#include "../loaders.h"
#include "helpers/json.hpp"

namespace dejavu {

    struct Repository {
        //unsigned id;
        std::string user;
        std::string project;
    };

    struct RepositoryHash {
        size_t operator ()(const Repository &r) const noexcept {
            return std::hash<std::string>()(r.user) << 32
                   | std::hash<std::string>()(r.project);
        }
    };

    struct RepositoryComp {
        bool operator()(const Repository &r1, const Repository &r2) const noexcept {
            return r1.project == r2.project
                   && r1.user == r2.user;
        }
    };

    void LoadRepositories(std::unordered_set<Repository, RepositoryHash, RepositoryComp> &repositories) {
        clock_t timer = clock();
        std::string task = "loading repositories for whom createdAt == 0";
        helpers::StartTask(task, timer);

        unsigned all_repositories = 0;
        unsigned incomplete_repositories = 0;

        ProjectLoader([&](unsigned id, std::string const & user,
                          std::string const & repo, uint64_t createdAt){

            ++all_repositories;

            if (createdAt == 0)
                return;

            Repository repository;
            repository.user = user;
            repository.project = repo;
            repositories.insert(repository);

            ++incomplete_repositories;
        });

        std::cerr << "Found " << incomplete_repositories
                  << " repositories missing a creation date "
                  << "(out of " << all_repositories << ")"
                  << std::endl;

        helpers::FinishTask(task, timer);
    }

    void SaveRepositories(std::unordered_set<Repository, RepositoryHash, RepositoryComp> const &repositories) {

        std::string output_path(DataDir.value() + OutputDir.value() + "/repositories-missing-creation-times.list");

        clock_t timer = clock();
        std::string task = "saving repository information to disk at '" + output_path + "'";
        helpers::StartTask(task, timer);

        size_t counter = 0;
        helpers::StartCounting(counter);

        std::ofstream file(output_path);
        if (!file.good()) {
            ERROR("Unable to open file " << output_path << " for writing");
        }

        for (Repository const &repository : repositories) {
            file << repository.user << "/" << repository.project << std::endl;
            helpers::Count(counter);
        }

        helpers::FinishCounting(counter, "repositories");
        helpers::FinishTask(task, timer);
    }

    void ExtractRepositoriesSansCreationTime(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_set<Repository, RepositoryHash, RepositoryComp> repositories;
        LoadRepositories(repositories);
        SaveRepositories(repositories);
    }

};