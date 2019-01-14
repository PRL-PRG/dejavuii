#include "simple-commands.h"

namespace dejavu {

    helpers::Option<std::string> InputFile("inputFile", "processed/files.csv", {"-if"}, false);
    helpers::Option<std::string> OutputFile("outputFile", "processed/files_sorted.csv", {"-of"}, false);


    void SortFiles(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputFile);
        settings.addOption(OutputFile);
        settings.parse(argc, argv);
        settings.check();
        int err = system(STR("sort -t , -k 1 -n " << DataRoot.value() << "/" << InputFile.value() << " > " << DataRoot.value() << "/" << OutputFile.value()).c_str());
        if (err == -1)
            throw std::runtime_error("Unable to sort files");
    }
    
}
