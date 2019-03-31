#include <cstdlib>
#include <iostream>


#include "helpers/helpers.h"
#include "helpers/commands.h"

#include "settings.h"
#include "commands.h"



namespace dejavu {

    helpers::Settings Settings;
    helpers::Option<std::string> DataDir("data", "/home/peta/ghgrabber_joined", {"-d"}, false);
    //    helpers::Option<std::string> DownloaderDir("downloader", "/array/dejavu/ghgrabber_distributed_take_4", false);
    //helpers::Option<std::string> DataDir("data", "/home/peta/ghgrabber_join", {"-d"}, false);
    helpers::Option<std::string> DownloaderDir("downloader", "/home/peta/xxxxxx", false);
    helpers::Option<std::string> TempDir("tmp", "/tmp", false);
    helpers::Option<unsigned> NumThreads("numThreads", 32, {"-n"}, false);
    helpers::Option<unsigned> Seed("seed", 0, false);
    
} // namespace dejavu


using namespace dejavu;

/** Initializes the commands available in the tool.
 */
void InitializeCommands() {
    new helpers::Command("help", helpers::Command::PrintHelp, "Displays help information");
    // TODO add command to run the downloader Konrad has implemented as a shell script
    new helpers::Command("join", Join, "Joins the information about the downloaded projects into the CSV files used for further processing.");
    new helpers::Command("detect-folder-clones", DetectFolderClones, "Detects folder clones across all projects and find their originals");
    new helpers::Command("detect-file-clones", DetectFileClones, "Detects file clones across all projects and find their originals");
}


/** The main function.
 */
int main(int argc, char * argv[]) {
    std::cerr << "OH HAI CAN I HAZ DEJAVU AGAINZ?" << std::endl;
    try {
        InitializeCommands();
        helpers::Command::Execute(argc, argv);
        std::cerr << "KTHXBYE!" << std::endl;
        return EXIT_SUCCESS;
    } catch (std::exception const & e) {
        std::cerr << "OHNOEZ: " << e.what() << std::endl; 
    } catch (...) {
        std::cerr << "OHNOEZ: BASEMENT CAT LURKZ IN UR CODE AND IT FAILZ." << std::endl;
    }
    return EXIT_FAILURE;
 
}
