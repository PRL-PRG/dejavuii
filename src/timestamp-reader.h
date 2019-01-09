#pragma once

#include <unordered_map>
#include "helpers/csv-reader.h"

typedef unsigned int commit_id_t;
typedef unsigned long timestamp_t;

typedef std::unordered_map<commit_id_t, timestamp_t> timestamp_map_t;

namespace dejavu {

    /**
     * Reads a CSV file to extract a mapping from commits to timestamps.
     *
     * Given a CSV file of the form (commit_id, timestamp, ...) this class
     * reads the CSV line-by-line and constructs a map of the form 
     * commit_id -> timestamp. The result of this is then accessible through 
     * getTimestamps.
     */
    class TimestampReader : public helpers::CSVReader {
    public:
        TimestampReader(const std::string path);
        void read();
        const timestamp_map_t &getTimestamps();

    protected:
        const std::string path;
        timestamp_map_t timestamps;

        void row(std::vector <std::string> &row);
    };

} // namespace
