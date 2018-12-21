#pragma once

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
        std::vector<path_id_t> path_ids;
        timestamp_t timestamp;
    };

    class CommitOrder : public helpers::CSVReader<> {

    public:
        CommitOrder(const std::string path, const timestamp_map_t & timestamps);
        void read();

    protected:
        // Input data: constructor parameters.
        const std::string path;
        const timestamp_map_t & timestamps;

        // Output data:
        // some kind of collection of graphs/orders;

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