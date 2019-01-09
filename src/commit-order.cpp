#include "commit-order.h"

#include <algorithm>
#include <map>

namespace dejavu {

    CommitOrder::CommitOrder(std::string const &input_path,
                             std::string const &output_path,
                             std::unordered_map<unsigned int, unsigned long> const &timestamps)
            : input_path(input_path),
              output_path(output_path),
              timestamps(std::move(timestamps)) {}

    void CommitOrder::read() {
        first_row = true;

        std::ofstream csv_file(output_path, std::ios::out | std::ios::trunc);
        csv_file.close();

        parse(input_path);
    }

    unsigned long CommitOrder::getTimestamp(unsigned int commit_id) {

        auto pair = timestamps.find(commit_id);
        if (pair == timestamps.end()) {
            std::cerr << "[BELGIUM] Did not find commit #" << commit_id
                      << " in commits." << std::endl;
            return 0; // FIXME
        }

        return pair->second;
    }

    CommitInfo const & CommitOrder::getCommit(unsigned int commit_id,
                                              unsigned int project_id,
                                              unsigned long timestamp) {

        auto pair = commits.find(timestamp);
        if (pair != commits.end()) {

            if (pair->second.commit_id == commit_id) {
                return pair->second;
            } else {
                std::cerr << "[BELGIUM] I looked for a commit with timestamp " << timestamp
                          << " and commit id " << commit_id << " but there was already a "
                          << "commit there with commit id " << pair->second.commit_id
                          << std::endl;
                return pair->second; // FIXME
            }
        }

        CommitInfo commit;
        commit.project_id = project_id;
        commit.commit_id = commit_id;
        commit.timestamp = timestamp;

        commits[timestamp] = commit;
        return commits[timestamp];
    }

    void CommitOrder::aggregateProjectInfo(unsigned int project_id,
                                           unsigned int path_id,
                                           unsigned int commit_id) {

        // First, we find the commit associated with this particular row.
        unsigned long timestamp = getTimestamp(commit_id);

        // Second, we check if we already have an aggregated commit associated
        // with this commit id. If we do not we need to create a new
        // aggregated_commit entry. Otherwise, we grab a reference to it.
        CommitInfo commit = getCommit(commit_id, project_id, timestamp);

        // Third, we add the file into the aggregated commit's file list.
        commit.path_ids.insert(path_id);
    }

    std::unordered_set<unsigned int> getCommonFiles(CommitInfo & a, CommitInfo & b) {
        std::unordered_set<unsigned int> intersection;
        set_intersection(a.path_ids.begin(), a.path_ids.end(),
                         b.path_ids.begin(), b.path_ids.end(),
                         inserter(intersection, intersection.begin()));
        return intersection;
    }

//    void writeOutToCSV(std::ofstream *csv_file, unsigned int project,
//                       unsigned int predecessor, unsigned int successor,
//                       std::unordered_set<unsigned int> *common_files) {
//
//        std::unordered_set<unsigned int>::iterator i;
//        for (i = common_files->begin(); i != common_files->end(); ++i) {
//            unsigned int path_id = *i;
//            (*csv_file) << project << ","
//                        << predecessor << ","
//                        << successor << ","
//                        << path_id
//                        << std::endl;
//        }
//    }

    /**
     * Once all of the data of a project are read in
     */
    void CommitOrder::processExistingData() {

        // Open the output file for appending.
        std::ofstream csv_file;
        csv_file.open(output_path, std::ios::out | std::ios::app);

        // Let's count relations.
        int relations = 0;

        std::unordered_map<unsigned int, unsigned int> file_and_the_commit_that_last_modified_it;
        for (auto & pair : commits) {
            CommitInfo info = pair.second;
            for (auto it = info.path_ids.begin(); it != info.path_ids.end(); ++it) {
                unsigned int path_id = *it;
                auto pair = file_and_the_commit_that_last_modified_it.find(path_id);
                if (pair != file_and_the_commit_that_last_modified_it.end()) {
                    csv_file << info.project_id << ","
                             << file_and_the_commit_that_last_modified_it[path_id] << ","
                             << info.commit_id << ","
                             << path_id
                             << std::endl;
                    relations++;
                }
                file_and_the_commit_that_last_modified_it[path_id] = info.commit_id;
            }
        }

//        // Create the order. Compare all commits within the project among each
//        // other.
//        for (auto & outer : commits) {
//            CommitInfo out = outer.second;
//
//            for (auto & inner : commits) {
//                CommitInfo in = inner.second;
//
//                // The commit neither precedes nor succeeds itself, the order
//                // relation is not reflexive.
//                if (out.commit_id == in.commit_id)
//                    continue;
//
//                // Check if the commits have any files in common. If they do not,
//                // they are not ordered.
//                std::unordered_set<unsigned int> common_files = getCommonFiles(in, out);
//                if (common_files.size() == 0)
//                    continue;
//
//                // Retrieve timestamps from the map gathered earlier.
//                unsigned long in_timestamp = getTimestamp(in.commit_id);
//                unsigned long out_timestamp = getTimestamp(out.commit_id);
//
//                // If the timestamps are the same, there is no order relation. I
//                // guess this probably doesn't happen very often, if at all.
//                if (in_timestamp == out_timestamp)
//                    continue;
//
//                // If the in timestamp precedes the out timestamp, we create that
//                // relationship. We output it to a CSV file.
//                if (in_timestamp < out_timestamp)
//                    writeOutToCSV(&csv_file, current_project,
//                                  in.commit_id, out.commit_id,
//                                  &common_files);
//
//                // If the in timestamp follows the out timestamp, we create that
//                // relationship. We output it to a CSV file.
//                if (in_timestamp > out_timestamp)
//                    writeOutToCSV(&csv_file, current_project,
//                                  out.commit_id, in.commit_id,
//                                  &common_files);
//
//                // Count relations.
//                relations += common_files.size();
//            }
//        }

        csv_file.close();

        std::cerr << "Written out " << relations << " relations for project #"
                  << current_project << std::endl;

        // Finally, remove all data aggregated so far.
        //for (auto & iterator : commits) {
        //    free(iterator.second);
        //}
        commits.clear();
    }

    void CommitOrder::row(std::vector<std::string> &row) {

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
}
