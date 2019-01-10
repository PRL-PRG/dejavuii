#include "simple-commands.h"

namespace dejavu {

    helpers::StringOption InputFile("inputFile", "processed/files.csv", {"-if"}, false);
    helpers::StringOption OutputFile("outputFile", "processed/files_sorted.csv", {"-of"}, false);


    void SortFiles(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputFile);
        settings.addOption(OutputFile);
        settings.parse(argc, argv);
        settings.check();
        int err = system(STR("sort -t , -k 1 -n " << DataRoot << "/" << InputFile << " > " << DataRoot << "/" << OutputFile).c_str());
        if (err == -1)
            throw std::runtime_error("Unable to sort files");
    }
    
}
