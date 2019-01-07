/*
 * Definitions of the output.
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
 * Project commit graph is a directed acyclic graph CGₚ(N,E) where:
 *    - N is the set of all commits in project P,
 *    - E is the set of edges st. E = FMOₚ.
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
 */

/*
 * Reading data from files.
 *
 * Assumption: I assume that `files.csv` is ordered so that all the commits from
 * any given project are grouped into a contiguous sequence. If this is not the
 * case, I will sort the file offline to ensure this.
 *
 * Update: I did have to sort:
 *
 *      sort -t , -k 1 -n files.csv > files_sorted.csv
 *
 * Assumption: the data for any one project fits in memory. I checked a few
 * projects off the top.
 *
 *          Project 1 is 138.5M lines,
 *                  2 is 138.5M also,
 *                  3 is 41,
 *                  4 is 12.3M,
 *                  5 is 916.
 *
 * There are 145.9M projects, BTW.
 *
 * The goal is to denormalize the data into the form:
 *
 *     (project id, commit id and info, list of modified files)
 *
 * Algorithm ideas.
 *
 * I   The naïve approach.
 *
 *     Read the data from `files.csv` so that I have all the data for a exactly
 *     a single project in memory. In memory I have a tuple (project id,
 *     path id, file hash id, commit id).
 *
 *     Then, open up `commits.csv` and read it sequentially until the commit
 *     information for all commits in memory is filled in.
 *
 *     Then, open up `paths.csv` and read it sequentially until the end to fill
 *     in the list of modified files for each commit.
 *
 *     Process data for the one project and output results. Rinse and repeat.
 *
 *     Reads entire `files.csv` once.
 *     Reads `commits.csv` as many times as projects, usually not the entire
 *     file.
 *     Reads entire `paths.csv` as many times as projects.
 *
 * II  The slightly more sophisticated naïve approach.
 *
 *     Read the data from `files.csv` so that I have a specific amount of data.
 *
 *     This data will have N projects. Keep N-1 projects and put everything
 *     regarding the Nth project into a buffer of some kind.
 *
 *     Then proceed like in I. At the end process data for all the N-1 projects
 *     and output the results. Take data for the Nth project out of the buffer.
 *     When new data will be read, it will be appended to the data taken from
 *     the buffer. Rinse and repeat.
 *
 * III The preprocessing approach.
 *
 *     This approach has two preprocessing stages where I produce new input
 *     files.
 *
 *     IIIa Preprocess `paths.csv` tagging each line with a project id. Sort
 *          the data according to project id. Save the data to
 *          `paths_with_projects.csv`.
 *
 *     IIIb Preprocess `commits.csv` likewise, producing
 *          `commits_with_projects.csv`.
 *
 *     Now we can merge `paths_with_projects.csv` and
 *     `commits_with_projects.csv` with sort-merge join. And since they will
 *     now be tagged with projects and sorted according to projects we can read
 *     in one project at a time and process them.
 *
 *     I think preprocessing IIIa and IIIb would best be done with something
 *     like csvkit, instead of implementing all of that in C++. So a simple
 *     bash script.
 *
 *
 */
#include <cstdlib>
#include <iostream>

#include "TimestampReader.h"
#include "CommitOrder.h"

using namespace std;
using namespace dejavu;

int main(int argc, char * argv[]) {
    string dir = "/data/dejavuii/data/processed/";

    TimestampReader timestamp_reader(dir + "commits.csv");
    timestamp_reader.read();
    const timestamp_map_t timestamps = timestamp_reader.getTimestamps();
    cerr << "Read " << timestamps.size() << " timestamps" << endl;

    CommitOrder order(/*dir +*/ "files_sorted.csv", timestamps);
    order.read();

    return EXIT_SUCCESS;
}
