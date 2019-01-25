#include "commit-order.h"
#include <src/objects.h>

#include <algorithm>
#include <map>

namespace dejavu {

    CalculateCommitOrder::CalculateCommitOrder(std::string const & input_path,
                             std::string const & output_path)
            : input_path(input_path),
              output_path(output_path) {}

    void CalculateCommitOrder::read() {
        first_row = true;

        std::ofstream csv_file(output_path, std::ios::out | std::ios::trunc);
        csv_file.close();

        parse(input_path, false);
    }

    void CalculateCommitOrder::aggregateProjectInfo(unsigned int project_id,
                                           unsigned int path_id,
                                           unsigned int commit_id) {

        // First, we find the timestamp associated with this commit ID.
        unsigned long timestamp = Commit::Get(commit_id)->time;

        // Second, we check if we already have any commits at this timestamp.
        auto pair = commits.find(timestamp);
        if (pair != commits.end()) {

            // If so, we add our new commit to the set of commits under that
            // timestamp.
            auto found = pair->second.find(commit_id);
            if (found != pair->second.end()) {
                CommitInfo commit = found->second;
                commit.path_ids.insert(path_id);
            } else {
                CommitInfo commit;

                commit.project_id = project_id;
                commit.commit_id = commit_id;
                commit.timestamp = timestamp;
                commit.path_ids.insert(path_id);

                pair->second[commit_id] = commit;
            }
        } else {
            // Otherwise, we create an empty set around the new commit and
            // insert it into our data.
            CommitInfo commit;

            commit.project_id = project_id;
            commit.commit_id = commit_id;
            commit.timestamp = timestamp;
            commit.path_ids.insert(path_id);

            std::unordered_map<unsigned int, CommitInfo> simultaneous;

            simultaneous[commit_id] = commit;
            commits[timestamp] = simultaneous;
        }
    }

    /**
     * Once all of the data of a project are read in, extract the commit
     * order.
     */
    void CalculateCommitOrder::processExistingData() {

        // Open the output file for appending.
        std::ofstream csv_file;
        csv_file.open(output_path, std::ios::out | std::ios::app);

        // Let's count relations.
        int relations = 0;

        // First we create a structure which we will use to keep track of
        // which commit modified which file. The key is the ID of a file,
        // while the value is the ID of commits. As we explore the commits
        // in the project in order of timestamps, we will update this
        // structure to tell us which commit modified a specific file most
        // recently from the point of view of the commit we are looking at.
        std::unordered_map<unsigned int, unsigned int> file_and_the_commit_that_last_modified_it;

        // Then, we start to iterate over all the commits in the project.
        // These commits are ordered by timestamp by virtue of being stored
        // in a timestamp -> commits ordered map, so, in effect, the algorithm
        // looks through them in order of timestamps.
        for (auto & pair : commits) {

            // Each timestamp holds at least one, but possibly many commits.
            // These commits are simultaneous. Since they are simultaneous,
            // they do not have relations between each other.
            std::unordered_map<unsigned int, CommitInfo> simultaneous_commits = pair.second;

            // We examine each of these simultaneous commits in isolation.
            for (auto cit = simultaneous_commits.begin(); cit != simultaneous_commits.end(); ++cit) {
                CommitInfo current_commit = cit->second;

                // For each file the currently examined commit modifies, we
                // use out absurdly named structure to find the commit that
                // modified this file previously.
                for (auto it = current_commit.path_ids.begin(); it != current_commit.path_ids.end(); ++it) {
                    unsigned int path_id = *it;
                    auto pair = file_and_the_commit_that_last_modified_it.find(path_id);

                    // If we find a commit that modified this file previously,
                    // we note down a relation between the previous commit and
                    // the current commit.
                    if (pair != file_and_the_commit_that_last_modified_it.end()) {
                        csv_file << current_commit.project_id << ","
                                 << file_and_the_commit_that_last_modified_it[path_id] << ","
                                 << current_commit.commit_id << ","
                                 << path_id
                                 << std::endl;
                        relations++;
                    }

                    // In either case, we store the current commit id in the
                    // ridiculously named data structure to indicate that this
                    // is the most recent commit that modified this file.
                    file_and_the_commit_that_last_modified_it[path_id] = current_commit.commit_id;
                }

            }
        }

        // Close the output file.
        csv_file.close();

        std::cerr << "Written out " << relations << " relations for project #"
                  << current_project << std::endl;

        // Finally, remove all data aggregated so far.
        //for (auto & iterator : commits) {
        //    free(iterator.second);
        //}
        commits.clear();
    }

    void CalculateCommitOrder::row(std::vector<std::string> &row) {

        // Get the basic data from the row, convert into appropriate types.
        unsigned int project_id = std::stoi(row[0]);
        unsigned int path_id = std::stoi(row[1]);
        unsigned int commit_id = std::stoi(row[3]);

        // If this is the first row, initialize the current project field.
        if (first_row) {
            current_project = project_id;
            first_row = false;

            std::cerr << "Reading project #" << project_id << std::endl;
        }

        // If the project changed, process all the data collected so far and
        // initialize a new project.
        if (project_id != current_project) {
            std::cerr << "Done reading project #" << current_project << std::endl;
            std::cerr << "Read " << commits.size() << " commits" << std::endl;

            std::cerr << "Processing commits for project #" << current_project
                      << std::endl;

            processExistingData();
            current_project = project_id;

            std::cerr << "Reading project #" << project_id << std::endl;
        }

        // Process the data from the current row.
        aggregateProjectInfo(project_id, path_id, commit_id);
    }


    void CommitOrder(int argc, char * argv[]) {
        std::string dir = "/data/dejavuii/data/processed/";
        std::string files_sorted = dir + "files_sorted.csv";
        std::string commit_order = dir + "commit_order.csv";
        std::string commits = dir + "commits.csv";

        std::cerr << "Reading timestamps from " << commits << std::endl;

//        TimestampReader timestamp_reader(commits);
//        timestamp_reader.read();
//        std::unordered_map<unsigned int, unsigned long> const timestamps = timestamp_reader.getTimestamps();

        Commit::ImportFrom(commits, false);

        //std::cerr << "Read " << Commit::Reader .size() << " timestamps" << std::endl;

        std::cerr << "Reading commit orders using " << files_sorted << std::endl;
        std::cerr << "Results will be written to " << commit_order << std::endl;

        CalculateCommitOrder order(files_sorted, commit_order);
        order.read();

        std::cerr << "Done." << std::endl;
    }
}
