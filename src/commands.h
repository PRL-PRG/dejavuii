#pragma once

#include "settings.h"

namespace dejavu {


    /** Verifies the data obtained from the ghgrabber that commit numbers match each other.
     */
    void VerifyGhGrabber(int argc, char * argv[]);

    /** Takes the data from the downloader, which includes non-js files and per-project redundancy uncompacted hashes being produced by the downloader and conpacts the hashes and cherrypicks the information we need removing the redundancies.
     */
    void Join(int argc, char * argv[]);

    /** The creation time join puts on projects is wrong (it is the oldest commit, which for forks is useless). This pass attempts to fix this by getting the proper (?) creation time from the ghtorrent database.
     */
    void PatchProjectsCreatedAt(int argc, char * argv[]);

    /** Verifies that the information in the dataset makes sense and creates a valid subset. Namely checks that the data in commit changes is coherent (i.e. no deletions of previously unknown files) and discrads projects for which it is not true that for each commit its parents are older.
        
        TODO does not deal with information we are not using for now (such as commit authors, etc.).
     */
    void Verify(int argc, char * argv[]);

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
    void FolderClonesFilter(int argc, char * argv[]);

    /** Calculates project level summary counts of paths, changes and deletions and node_modules paths, changes and deletions.
     */
    void NPMModuleCounts(int argc, char * argv[] );

    /** Analyzes the historical information of folder clones.
     */
    void FolderCloneHistoryAnalysis(int argc, char * argv[]);

    /** Aggregates the number of total and cloned live files per time.
     */
    void ClonesOverTime(int argc, char * argv[]);

    /** Filters out files in node_modules.
     */
    void FilterNPM(int argc, char * argv[]);

    /** Calculates interesting stats.
     */
    void Stats(int argc, char * argv[]);

    /** Reads the input data and determines all file clone candidates and finds their originals.

        This command is intended to run in a batch mode and as such does not provide for incremental analysis, or for caching the results. Each run will start fresh since the scenario is that when new projects are added to the corpus, they can be the new originals of any possible clone cluster.
     */
    void DetectFileClones(int argc, char * argv[]);

    /** Reads the projects.csv file extracted form GHTorrent and extracts forked and non-forked, non-deleted, Javascript projects.
     *
     */
    void ExtractJSProjects(int argc, char * argv[]);
} // namespace dejavu
