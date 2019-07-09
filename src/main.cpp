#include <cstdlib>
#include <iostream>


#include "helpers/helpers.h"
#include "helpers/commands.h"

#include "settings.h"
#include "commands.h"



namespace dejavu {

    helpers::Settings Settings;
    helpers::Option<std::string> DataDir("data", "/data/dejavuii/joined", {"-d"}, false);
    helpers::Option<std::string> OutputDir("outputDir", "", {"-o"}, true);
    helpers::Option<std::string> DownloaderDir("downloader", "/array/dejavu/ghgrabber_distributed_take_4", false);
    helpers::Option<std::string> TempDir("tmp", "/tmp", false);
    helpers::Option<unsigned> NumThreads("numThreads", 8, {"-n"}, false);
    helpers::Option<unsigned> Seed("seed", 0, false);
    helpers::Option<unsigned> Threshold("threshold", 2, {"-t"}, false);
    helpers::Option<unsigned> Pct("pct", 5, {"-pct"}, false);
    helpers::Option<std::string> GhtDir("ghtorrent", "", {"-ght"}, true);
    helpers::Option<unsigned> IgnoreFolderOriginals("ignoreFolderOriginals", 0, false);
    
} // namespace dejavu


using namespace dejavu;

/** Initializes the commands available in the tool.
 */
void InitializeCommands() {
    new helpers::Command("help", helpers::Command::PrintHelp, "Displays help information");
    // TODO add command to run the downloader Konrad has implemented as a shell script
    new helpers::Command("verify-ghgrabber", VerifyGhGrabber, "Verifies the integrity of the data obtained by the ghgrabber");
    new helpers::Command("join", Join, "Joins the information about the downloaded projects into the CSV files used for further processing.");
    new helpers::Command("detect-forks", DetectForks, "Detects projects that are forked or cloned other repositories.");
    new helpers::Command("patch-projects-createdAt", PatchProjectsCreatedAt, "Patches project createAt times from ghtorrent data.");
    new helpers::Command("verify", Verify, "Verifies the joined dataset and creates a subset containing valid data only.");
    new helpers::Command("time-subset", TimeSubset, "Creates time bound subset of the data");
    new helpers::Command("npm-counts", NPMModuleCounts, "Calculates summaries for projects wrt their paths, changes and node_modules paths and changes.");
    new helpers::Command("detect-folder-clones", DetectFolderClones, "Detects folder clones across all projects and find their originals");
    new helpers::Command("find-folder-originals", FindFolderOriginals, "Finds folder originals for previously detected clone candidates");
    new helpers::Command("filter-folder-clones", FolderClonesFilter, "Filters folder clones so that different subsets of same original use same clone id");
    new helpers::Command("folder-clones-history", FolderCloneHistoryAnalysis, "Detects folder clones across all projects and find their originals");
    new helpers::Command("clones-over-time", ClonesOverTime, "Aggregates clone stats over time");
    new helpers::Command("npm-filter", NPMFilter, "Filters node_modules files");
    new helpers::Command("stats", Stats, "Calculates interesting stats");
    new helpers::Command("detect-file-clones", DetectFileClones, "Detects file clones across all projects and find their originals");
    new helpers::Command("extract-js-projects", ExtractJSProjects, "Extracts JS projects from a GHTorrent projects.csv file which are not deleted and splits them into forked and non-forked.");
    new helpers::Command("npm-summary", NPMSummary, "Produces a summary of NPM packages");
    new helpers::Command("npm-download", NPMDownload, "Downloads project.json files from NPM packages");
    new helpers::Command("npm-github-urls", ExtractRepositoriesFromNPMProjects, "Prepares a list of GitHub URLs for NPM packages from their package.json files");
    new helpers::Command("download-repository-info", DownloadRepositoryInfo, "Downloads a JSON file containint basic info about the repository (createdAt, etc.) for each specified project");

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
