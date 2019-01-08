/**
 * The program extracts commit order from input data.
 * 
 * The input data consists of two CSV files:
 *   (a) commits.csv containing the metadata information of commits of the form
 *       (commit_id, timestamp, ...),
 *   (b) files_sorted.csv containing the information about files modified by
 *       commits of the form (project_id, path_id, hash_id, commit_id). This
 *       file must be sorted according to project_id. 
 * 
 * The output data is a CSV file that describes the order among commits within
 * projects. The CSV takes the form (project_id, commit_id1, commit_id2,
 * path_id) which indicates that commit_id1 < commit_id2 in project_id. The
 * file_id field indicates that both commits modify this file. If there are
 * more files that the commits have in common, the program outputs multiple
 * lines. The file is sorted by project_id. 
 *
 * Semi-formal definitions
 *
 * Given two commits X and Y, commit X fm-precedes commit Y iff:
 *   (a) time(X) < time(Y), and
 *   (b) files(X) ∩ filex(Y) ≠ ∅.
 *
 * File modification commit order FMO(S) is a partial order on any set of
 * commits S st. ∀X ∊ S and Y ∊ S,  (X, Y) ∊ FMO(S) iff X fm-precedes Y.
 *
 * Project file modification commit order FMOₚ is the order FMO(S) where S
 * is the set of all commits st. X ∊ S iff project(X) = P.
 *
 * Example input:
 *    - C₁ modifies file  A    at time T₁,
 *    - C₂ modifies file  B    at time T₂,
 *    - C₃ modifies file  C    at time T₃,
 *    - C₄ modifies file  A    at time T₄,
 *    - C₅ modifies files A, B at time T₅,
 *    - C₆ modifies file  B    at time T₆.
 *
 * Output FMO: [(C₁, C₄), (C₂, C₅), (C₄, C₅), (C₅, C₆)].
 *
 * Practical considerations
 * 
 * The file files_sorted.csv is not in the input dataset, I constructed it by
 * sorting files.csv as follows:
 *
 *      sort -t , -k 1 -n files.csv > files_sorted.csv
 * 
 * This program tries to minimize memory usage by reading in input data one
 * project at a time. Specifically, the program reads in the entirety of
 * commits.csv at ~2.5G, and then a chunk of files.csv/files_sorted.csv
 * containing the commits of a single project. I checked a few projects off the
 * top of the file to see how big these chunks are:
 *
 *          Project 1 is 138.5M lines,
 *                  2 is 138.5M also,
 *                  3 is 41,
 *                  4 is 12.3M,
 *                  5 is 916.
 *
 * There are 145.9M projects, BTW.
 *
 * If there are three commits, 0, 1, and 2 that all modify the same file F, and
 * where timestamp(0) < timestamp(1) < timestamp(2) the program outputs:
 *
 *      P,0,1,F
 *      P,0,2,F
 *      P,1,2,F 
 *
 * The redundant second line could be eliminated, but currently is not.
 *
 */

#include <cstdlib>
#include <iostream>

//#include "helpers/helpers.h"
//#include "helpers/csv-reader.h"

#include "TimestampReader.h"
#include "CommitOrder.h"

using namespace std;
using namespace dejavu;

int main(int argc, char * argv[]) {
    string dir = "/data/dejavuii/data/processed/";

    cerr << "Reading timestamps from " << (dir + "commits.csv")  << endl;
    TimestampReader timestamp_reader(dir + "commits.csv");
    timestamp_reader.read();
    const timestamp_map_t timestamps = timestamp_reader.getTimestamps();
    cerr << "Read " << timestamps.size() << " timestamps" << endl;

    cerr << "Reading commit orders using " << (/*dir +*/ "files_sorted.csv")  << endl;
    cerr << "Results will be written to " << (/*dir +*/ "commit_order.csv")  << endl;
    CommitOrder order(/*dir +*/ "files_sorted.csv", /*dir +*/ "commit_order.csv", timestamps);
    order.read();
    cerr << "Done." << endl;

    return EXIT_SUCCESS;
}
