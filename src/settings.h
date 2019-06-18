#pragma once

#include "helpers/settings.h"

namespace dejavu {

    /** The settings object.

        The declarations below point to various settings that different commands of the program can use.

        NOTE All settings should be declared here and care should be taken that we have as few of the settings as possible. 
     */
    extern helpers::Settings Settings;

    /** Directory where to look for the csv files DejaVu II operates on. 
     */
    extern helpers::Option<std::string> DataDir;

    /** Generic directory for outputs.
     */
    extern helpers::Option<std::string> OutputDir;

    /** Directory where the downloader script dumped its results.
     */
    extern helpers::Option<std::string> DownloaderDir;
    
    /** Directory for temporary storage. 
     */
    extern helpers::Option<std::string> TempDir;

    /** Number of threads to use for parallel processing.
     */
    extern helpers::Option<unsigned> NumThreads;

    /** Random seed to be used for any operations requiring random numbers. 
     */
    extern helpers::Option<unsigned> Seed;

    /** threshold for various things. See the commands which use it for more details. 
     */
    extern helpers::Option<unsigned> Threshold;

    /** Generic percentage value.
     */
    extern helpers::Option<unsigned> Pct;

    /** Location of a ghtorrent output data.
     */
    extern helpers::Option<std::string> GhtDir;

    /** When true, folder clones are considered clones even if they are their own originals, which allows measurement of all clone candidates.
     */
    extern helpers::Option<unsigned> IgnoreFolderOriginals;
    
} // dejavu
