#include <unordered_map>
#include "timestamp-reader.h"

namespace dejavu {

    TimestampReader::TimestampReader(std::string const &path) : path(path) {}

    void TimestampReader::read() {
        parse(path);
    }

    void TimestampReader::row(std::vector<std::string> &row) {
        unsigned int commit_id = atoi(row[0].c_str());
        unsigned long timestamp = atol(row[1].c_str());
        timestamps[commit_id] = timestamp;
    }

    std::unordered_map<unsigned int, unsigned long> const &TimestampReader::getTimestamps() {
        return timestamps;
    }
}