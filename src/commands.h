#pragma once

#include "settings.h"

namespace dejavu {


    /** Verifies the data obtained from the ghgrabber that commit numbers match each other.
     */
    void VerifyGhGrabber(int argc, char * argv[]);

    /** Takes the data from the downloader, which includes non-js files and per-project redundancy uncompacted hashes being produced by the downloader and conpacts the hashes and cherrypicks the information we need removing the redundancies.
     */
    void Join(int argc, char * argv[]);

    /** Calculates summary changes for all commits in the projects.
     */
    void AllCommitsSummary(int argc, char * argv[]);

    /** Detects projects that are forks.
     */
    void DetectForks(int argc, char * argv[]);


    /** Filters given projects and their contents out of the dataset.
     */
    void FilterProjects(int argc, char * argv[]);
    
    /** The creation time join puts on projects is wrong (it is the oldest commit, which for forks is useless). This pass attempts to fix this by getting the proper (?) creation time from the ghtorrent database.
     */
    void PatchProjectsCreatedAt(int argc, char * argv[]);

    /** Finds which projects are using node.js by looking whether they have package.json file. 
     */
    void NPMUsingProjects(int argc, char * argv[]);

    /** Verifies that the information in the dataset makes sense and creates a valid subset. Namely checks that the data in commit changes is coherent (i.e. no deletions of previously unknown files) and discrads projects for which it is not true that for each commit its parents are older.
        
        TODO does not deal with information we are not using for now (such as commit authors, etc.).
     */
    void Verify(int argc, char * argv[]);

    /** Detects projects older than given date.
     */
    void DetectOldProjects(int argc, char * argv[]);

    /** Calculates whether given project was active in given year with different granularities.
     */        
    void ActiveProjectsYears(int argc, char * argv[]);

    /** Calculates weekly activity summary for projects
     */        
    void ActiveProjectsWeeks(int argc, char * argv[]);

    /** Creates subset of the dataset based on the time of events.

        This subset is faster to work on, while preserving as much of the properties of the original dataset. An alternative would be to create a subset in terms of say projects, but that would mean that we would ignore the idea that we see all originals.
     */
    void TimeSubset(int argc, char * argv[]);


    void DetectFileClones(int argc, char * argv[]);
    
    /** Reads the input data and determines all folder clone candidates and finds their originals.

        This command is intended to run in a batch mode and as such does not provide for incremental analysis, or for caching the results. Each run will start fresh since the scenario is that when new projects are added to the corpus, they can be the new originals of any possible folder clones. 
     */
    void DetectFolderClones(int argc, char * argv[]);

    /** Finds originals for previously detected folder clone candidates.
     */
    void FindFolderOriginals(int argc, char * argv[]);

    /** Filters the folder clones so that clones of different subsets still use the same original.  
     */
    void FolderClonesClean(int argc, char * argv[]);

    /** Splits the folder clones into project to project, project to folder and folder to folder. Also discards intraproject clones.
     */
    void SplitFolderClones(int argc, char * argv[]);

    /** Filters folder clones from the dataset.
     */
    void FilterFolderClones(int argc, char * argv[]);

    /** Analyzes the behavior of the folder clones, such as divergence and synchronicity.
     */
    void FolderClonesBehavior(int argc, char * argv[]);

    /** Finds file clones and their originals. 
     */
    void DetectFileClones(int argc, char * argv[]);

    /** The final breakdown of the remaining files after file clones have been removed.
     */
    void FinalBreakdown(int argc, char * argv[]);


    /** Calculates project lifespans - i.e. time of oldest and youngest commits recorded.
     */
    void ProjectLifespan(int argc, char * argv[]);


    /** Calculates for each project the number of committers and authors.
     */
    void ProjectAuthors(int argc, char * argv[]);

    /** Calculates file duplication and other stats per project.
     */
    void FileDuplicationPerProject(int argc, char * argv[]);
    


    /** Calculates the projects history table.
     */
    void HistoryOverview(int argc, char * argv[]);

    /** Expands the smaller history overview to contain datapoints from the larger one for easy merging.
     */
    void HistoryOverviewExpander(int argc, char * argv[]);

    /** Same as history overview, but uses paths as a guide.
     */
    void HistoryPaths(int argc, char * argv[]);

    

    /** Calculates project level summary counts of paths, changes and deletions and node_modules paths, changes and deletions.
     */
    void NPMModuleCounts(int argc, char * argv[] );

    /** Analyzes the historical information of folder clones.
     */
    void FolderCloneHistoryAnalysis(int argc, char * argv[]);

    /** Aggregates the number of total and cloned live files per time.
     */
    void ClonesOverTime(int argc, char * argv[]);

    /** Calculates interesting stats.
     */
    void Stats(int argc, char * argv[]);

    /** Reads the input data and determines all file clone candidates and finds their originals.

        This command is intended to run in a batch mode and as such does not provide for incremental analysis, or for caching the results. Each run will start fresh since the scenario is that when new projects are added to the corpus, they can be the new originals of any possible clone cluster.
     */
    void DetectFileClones2(int argc, char * argv[]);

    /** Reads the projects.csv file extracted form GHTorrent and extracts forked and non-forked, non-deleted, Javascript projects.
     *
     */
    void ExtractJSProjects(int argc, char * argv[]);

    /** Produces a summary of the NPM packages and projects status.
     */
    void NPMSummary(int argc, char * argv[]);

    /** Filters out files in node_modules.
     */
    void NPMFilter(int argc, char * argv[]);

    
    /** Downloads project.json files from NPM packages.
     */
    void NPMDownload(int argc, char * argv[]);

    /** Prepares a list of GitHub URLs for NPM packages from their package.json files.
     */
    void ExtractRepositoriesFromNPMProjects(int argc, char * argv[]);

    /** Downloads a JSON file containint basic info about the repository (createdAt, etc.) for each specified project
     */
    void DownloadGithubMetadata(int argc, char * argv[]);

    /**
     *
     */
    void ExtractRepositoriesSansCreationTime(int argc, char * argv[]);



    /** Downloads contents of actual files.
     */
    void DownloadContents(int argc, char * argv[]);



    /** Calculates active projects summaries.
     */
    void ActiveProjectsSummary(int argc, char * argv[]);

    /** Filters out projects that have commits before 2008 and removes commits newer than 2018 from the rest.
     */
    void Filter2008to2018(int argc, char * argv[]);

    /** Collects the metadata from previously downloaded github API and outputs a table summary for that.
     */
    void CollectMetadata(int argc, char * argv[]);




    void tmp_TranslateDownloadedMetadata(int argc, char * argv[]);
} // namespace dejavu
