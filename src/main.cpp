#include <cstdlib>
#include <iostream>


#include "helpers/helpers.h"
#include "helpers/commands.h"

#include "settings.h"
#include "commands.h"



namespace dejavu {

    helpers::Settings Settings;
    helpers::Option<std::string> DataDir("data", "/data/dejavuii/joined", {"-d"}, false);
    helpers::Option<std::string> Input("input", "", {"-i"}, true);
    helpers::Option<std::string> OutputDir("outputDir", "", {"-o"}, true);
    helpers::Option<std::string> Filter("filter","",{"-filter"}, true);
    helpers::Option<std::string> DownloaderDir("downloader", "/array/dejavu/ghgrabber_distributed_take_4", false);
    helpers::Option<std::string> TempDir("tmp", "/tmp", false);
    helpers::Option<unsigned> NumThreads("numThreads", 8, {"-n"}, false);
    helpers::Option<unsigned> Seed("seed", 0, false);
    helpers::Option<unsigned> Threshold("threshold", 2, {"-t"}, false);
    helpers::Option<unsigned> Pct("pct", 5, {"-pct"}, false);
    helpers::Option<std::string> GhtDir("ghtorrent", "", {"-ght"}, true);
    helpers::Option<unsigned> IgnoreFolderOriginals("ignoreFolderOriginals", 0, false);
    helpers::Option<std::string> GitHubPersonalAccessToken("GitHubPersonalAccessToken", "", {"-auth"}, false);
    helpers::Option<std::string> RepositoryList("RepositoryList",
                                                "/data/dejavuii/verified/npm-packages-missing.list",
                                                {"-repos"}, false);
    
} // namespace dejavu


// TODO how much data we need to answer and with what accuracy

using namespace dejavu;

/** Initializes the commands available in the tool.
 */
void InitializeCommands() {
    new helpers::Command("help", helpers::Command::PrintHelp, "Displays help information");
    // these are the commands that make it to our pipeline v2. Each of these commands must be in src/commands as a separate cpp file of the same name as the command and there should be an extended description of the command at the top of the file. 
    // TODO add command to run the downloader Konrad has implemented as a shell script
    new helpers::Command("join", Join, "Joins the information about the downloaded projects into the CSV files used for further processing.");

    new helpers::Command("patch-projects-createdAt", PatchProjectsCreatedAt, "Patches project createAt times from ghtorrent data.");
    new helpers::Command("download-github-metadata", DownloadGithubMetadata, "Downloads a JSON file containint basic info about the repository (createdAt, etc.) for each specified project");
    
    new helpers::Command("verify", Verify, "Verifies the joined dataset and creates a subset containing valid data only.");
    new helpers::Command("detect-old-projects", DetectOldProjects, "Detects projects with commits older than given threshold");

    new helpers::Command("active-projects-years", ActiveProjectsYears, "Calculates numbers of active projects with yearly granularity");
    new helpers::Command("active-projects-weeks", ActiveProjectsWeeks, "Calculates weekly activity summary for projects");
    
    // TODO Here we should patch the project's createdAt times, but we do not have the data yet, so we are working on later steps for now
    new helpers::Command("npm-summary", NPMSummary, "Produces a summary of NPM packages");
    new helpers::Command("npm-filter", NPMFilter, "Filters node_modules files");
    new helpers::Command("npm-using-projects", NPMUsingProjects, "Determine which projects use node.js");
    // TODO check that there is a prefix of commits that is shared, not just a commit
    new helpers::Command("detect-forks", DetectForks, "Detects projects that are forked or cloned other repositories.");
    new helpers::Command("filter-projects", FilterProjects, "Filters given projects and their contents from the dataset.");
    new helpers::Command("download-contents", DownloadContents, "Downloads contents of selected files.");



    // folder clones pipeline
    new helpers::Command("detect-folder-clones", DetectFolderClones, "Detects folder clones across all projects and find their originals");
    new helpers::Command("find-folder-originals", FindFolderOriginals, "Finds folder originals for previously detected clone candidates");
    new helpers::Command("clean-folder-clones", FolderClonesClean, "Cleans folder clones so that different subsets of same original use same clone id");
    new helpers::Command("filter-folder-clones", FilterFolderClones, "Filters changes to the folder clones from the dataset.");
    new helpers::Command("folder-clones-behavior", FolderClonesBehavior, "Analyzes the behavior of folder clones over time");

    // file clones
    new helpers::Command("detect-file-clones", DetectFileClones, "Detects the file clones");

    new helpers::Command("final-breakdown", FinalBreakdown, "Detects the file clones");





    
    new helpers::Command("project-lifespan", ProjectLifespan, "Calculates project lifespans");

    new helpers::Command("history-overview", HistoryOverview, "Calculates projects history summary");
    new helpers::Command("history-overview-expander", HistoryOverviewExpander, "Expands the smaller history overview to contain datapoints from the larger");

    new helpers::Command("history-paths", HistoryPaths, "Calculates projects history in terms of paths unique within a project");
    

    

    
    // temp stuff
    new helpers::Command("translate-metadata", tmp_TranslateDownloadedMetadata, "Translates downloaded metadata into a more pleasing format");
    
    // These are other commands, legacy stuff, sandboxes, etc.
    
    new helpers::Command("npm-counts", NPMModuleCounts, "Calculates summaries for projects wrt their paths, changes and node_modules paths and changes."); // TODO should be deleted[<0;160;44M]
    new helpers::Command("verify-ghgrabber", VerifyGhGrabber, "Verifies the integrity of the data obtained by the ghgrabber");
    new helpers::Command("time-subset", TimeSubset, "Creates time bound subset of the data");
    new helpers::Command("folder-clones-history", FolderCloneHistoryAnalysis, "Detects folder clones across all projects and find their originals");
    new helpers::Command("clones-over-time", ClonesOverTime, "Aggregates clone stats over time");
    new helpers::Command("stats", Stats, "Calculates interesting stats");
    new helpers::Command("detect-file-clones2", DetectFileClones2, "Detects file clones across all projects and find their originals");
    new helpers::Command("extract-js-projects", ExtractJSProjects, "Extracts JS projects from a GHTorrent projects.csv file which are not deleted and splits them into forked and non-forked.");
    new helpers::Command("npm-download", NPMDownload, "Downloads project.json files from NPM packages");
    new helpers::Command("npm-github-urls", ExtractRepositoriesFromNPMProjects, "Prepares a list of GitHub URLs for NPM packages from their package.json files");
    new helpers::Command("extract-repositories-sans-creation-time", ExtractRepositoriesSansCreationTime, "Extracts a list of repositories for which we are missing the createdAt attribute");
}


/** The main function.
 */
int main(int argc, char * argv[]) {
    std::cerr << "GIT_COMMIT " << helpers::Exec("git rev-parse HEAD", ".") << std::endl;
    std::cerr << "OH HAI CAN I HAZ DEJAVU AGAINZ?" << std::endl;
    size_t start = helpers::SteadyClockMillis();
    try {
        InitializeCommands();
        helpers::Command::Execute(argc, argv);
        std::cerr << "KTHXBYE!" << std::endl;
        std::cerr << "TOTAL_SECONDS " << ((helpers::SteadyClockMillis() - start) / 1000) << std::endl;
        return EXIT_SUCCESS;
    } catch (std::exception const & e) {
        std::cerr << "OHNOEZ: " << e.what() << std::endl; 
    } catch (...) {
        std::cerr << "OHNOEZ: BASEMENT CAT LURKZ IN UR CODE AND IT FAILZ." << std::endl;
    }
    return EXIT_FAILURE;
}
