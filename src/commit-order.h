#pragma once

#include <set>
#include <vector>
#include <unordered_map>
#include "helpers/csv-reader.h"
#include "timestamp-reader.h"

namespace dejavu {

    struct CommitInfo {
        unsigned int commit_id;
        unsigned int project_id;
        std::set<unsigned int> path_ids;
        unsigned long timestamp;
    };

    /**
     * Reads a CSV file to extract the order of commits in projects.
     *
     * Given a CSV file of the form (project_id, path_id, hash_id, commit_id)
     * and a map of timestamps (commit_id -> timestamp) it extracts the order
     * of commits in each of the projects. We assume the CSV file is ordered by
     * project_id.
     *
     * The input CSV is read until all of the commits of a single project are
     * in memory (ie. read until the project ID changes). When all of a
     * projects commits are in memory, they are compared among each other.
     * If any two commits modify some common set of files, then they are
     * ordered by their timestamp. There is no order relation if the timestamps
     * are the same or if the commits do not impact any files in common.
     *
     * When the commit order is created, the commit data is removed from
     * memory, and the reader proceeds to read in another whole project.
     *
     */
    class CommitOrder : public helpers::CSVReader {

    public:
        CommitOrder(const std::string input_path, 
                    const std::string output_path, 
                    const std::unordered_map<unsigned int, unsigned long> & timestamps);
        void read();

    protected:
        // Input data: constructor parameters.
        const std::string input_path;
        const std::string output_path;
        const std::unordered_map<unsigned int, unsigned long> & timestamps;

        // Output data:
        // const map<project_id, set<order_elem_t>> orders;

        // Internal processing data to carry information between calls to row().
        bool first_row;
        unsigned int current_project;
        std::unordered_map<unsigned int, CommitInfo *> commits;

        // Auxiliary functions.
        void row(std::vector<std::string> & row);
        void aggregate_project_info(unsigned int project_id, unsigned int path_id,
                                    unsigned int commit_id);
        void process_existing_data();
        unsigned long getTimestamp(unsigned int commit_id);
        CommitInfo * getCommit(unsigned int commit_id, unsigned int project_id,
                                unsigned long timestamp);
    };

} // namespace
