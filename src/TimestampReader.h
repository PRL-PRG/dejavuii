#pragma once

#include <unordered_map>
#include "helpers/csv-reader.h"

typedef unsigned int commit_id_t;
typedef unsigned long timestamp_t;

typedef std::unordered_map<commit_id_t, timestamp_t> timestamp_map_t;

namespace dejavu {

    class TimestampReader : public helpers::CSVReader<> {
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
