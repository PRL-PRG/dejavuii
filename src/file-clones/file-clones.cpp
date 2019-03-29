#include "file-clones.h"
#include "helpers/helpers.h"
#include "src/settings.h"
#include "version.h"

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed",
                                               false);
        helpers::Option<std::string> ProjectDir("projectDir", "/processed",
                                                false);
    }

    void FileClones(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(OutputDir);
        settings.addOption(ProjectDir);
        settings.parse(argc, argv);
        settings.check();

        // TODO
    }
}