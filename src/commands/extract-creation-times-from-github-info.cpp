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

    };

    void ExtractCreationTimesFromGitHubInfo(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        //std::unordered_set<Repository, RepositoryHash, RepositoryComp> repositories;
//        LoadRepositories(repositories);
//        SaveRepositories(repositories);
    }

};