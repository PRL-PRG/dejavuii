#pragma once

#include <unordered_map>
#include "helpers/csv-reader.h"

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
        TimestampReader(std::string const path);
        void read();
        std::unordered_map<unsigned int, unsigned long> const & getTimestamps();

    protected:
        std::string const path;
        std::unordered_map<unsigned int, unsigned long> timestamps;

        void row(std::vector <std::string> &row);
    };

} // namespace
