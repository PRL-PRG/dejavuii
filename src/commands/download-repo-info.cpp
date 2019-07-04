#include <stdio.h>
#include <curl/curl.h>
#include <stdlib.h>
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

    namespace {
        helpers::Option <std::string> RepositoryList("RepositoryList",
                                                     "/data/dejavuii/verified/npm-packages-missing.list",
                                                     false);
    };

    struct Repository {
        std::string user;
        std::string project;

        bool operator ==(const Repository &r) const noexcept {
            return (user == r.user) && (project == r.project);
        }
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
        size_t total_repositories = 0;
        std::string task = "loading repository user and project names";
        helpers::StartTask(task, timer);

        auto f = [&](std::string const &user, std::string const &project) {
            ++total_repositories;
            Repository repo;
            repo.user = user;
            repo.project = project;
            repositories.insert(repo);
        };

        RepositoryListLoader(RepositoryList.value(), f);

        std::cerr << "Repositories loaded: " << total_repositories << std::endl;
        std::cerr << "Unique repositories loaded: " << repositories.size() << std::endl;

        helpers::FinishTask(task, timer);
    }

    void DownloadRepositoryInfo(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(RepositoryList);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_set<Repository, RepositoryHash, RepositoryComp> repositories;
        LoadRepositories(repositories);

        for (const Repository &r : repositories) {
            std::cerr << r.user << " -> " << r.project;
        }
    }
};


