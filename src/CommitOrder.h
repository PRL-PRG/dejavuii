#pragma once

#include <set>
#include <vector>
#include <unordered_map>
#include "helpers/csv-reader.h"
#include "TimestampReader.h"

namespace dejavu {

    typedef unsigned int project_id_t;
    typedef unsigned int path_id_t;

    struct commit_info {
        commit_id_t commit_id;
        project_id_t project_id;
        std::set<path_id_t> path_ids;
        timestamp_t timestamp;
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
    class CommitOrder : public helpers::CSVReader<> {

    public:
        CommitOrder(const std::string path, const timestamp_map_t & timestamps);
        void read();

    protected:
        // Input data: constructor parameters.
        const std::string path;
        const timestamp_map_t & timestamps;

        // Output data:
        // const map<project_id, set<order_elem_t>> orders;

        // Internal processing data to carry information between calls to row().
        bool first_row;
        project_id_t current_project;
        std::unordered_map<commit_id_t, commit_info *> commits;

        // Auxiliary functions.
        void row(std::vector<std::string> & row);
        void aggregate_project_info(project_id_t project_id, path_id_t path_id,
                                    commit_id_t commit_id);
        void process_existing_data();
        timestamp_t getTimestamp(commit_id_t commit_id);
        commit_info * getCommit(commit_id_t commit_id, project_id_t project_id,
                                timestamp_t timestamp);
    };

} // namespace
