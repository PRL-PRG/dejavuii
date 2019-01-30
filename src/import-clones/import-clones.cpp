#include "helpers/helpers.h"

#include "../settings.h"

#include "src/objects.h"


#include "import-clones.h"

namespace dejavu {


    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", false);
    }
    
    void ImportClones(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.parse(argc, argv);
        settings.check();
        std::string idir = DataRoot.value() + InputDir.value();

        LoadDataset(idir);
    }
    
    void LoadDataset(std::string const & dataset) {
        Project::ImportFrom(STR(dataset << "/projects.csv"), false);
        Commit::ImportFrom(STR(dataset << "/commits.csv"), false);
        Path::ImportFrom(STR(dataset << "/paths.csv"));
        FileHash::ImportFrom(STR(dataset << "/fileHashes.csv"), false);
        ImportFiles(STR(dataset << "/files.csv"));
    }

    
    
} // namespace dejavu
