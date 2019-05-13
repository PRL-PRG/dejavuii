#pragma once

#include "settings.h"

namespace dejavu {


    /** Verifies the data obtained from the ghgrabber that commit numbers match each other.
     */
    void Verify(int argc, char * argv[]);

    /** Takes the data from the downloader, which includes non-js files and per-project redundancy uncompacted hashes being produced by the downloader and conpacts the hashes and cherrypicks the information we need removing the redundancies.
     */
    void Join(int argc, char * argv[]);

    /** Reads the input data and determines all folder clone candidates and finds their originals.

        This command is intended to run in a batch mode and as such does not provide for incremental analysis, or for caching the results. Each run will start fresh since the scenario is that when new projects are added to the corpus, they can be the new originals of any possible folder clones. 
     */
    void DetectFolderClones(int argc, char * argv[]);

    /** Reads the input data and determines all file clone candidates and finds their originals.

        This command is intended to run in a batch mode and as such does not provide for incremental analysis, or for caching the results. Each run will start fresh since the scenario is that when new projects are added to the corpus, they can be the new originals of any possible clone cluster.
     */
    void DetectFileClones(int argc, char * argv[]);

    /** Reads the projects.csv file extracted form GHTorrent and extracts forked and non-forked, non-deleted, Javascript projects.
     *
     */
    void ExtractJSProjects(int argc, char * argv[]);
} // namespace dejavu
