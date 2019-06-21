#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {

    };

    void NPMDownload(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();


    }
    
} // namespace dejavu
