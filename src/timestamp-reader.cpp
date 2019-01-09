#include <unordered_map>
#include "timestamp-reader.h"

namespace dejavu {

    TimestampReader::TimestampReader(std::string const &path) : path(path) {}

    void TimestampReader::read() {
        parse(path);
    }

    void TimestampReader::row(std::vector<std::string> &row) {
        unsigned int commit_id = std::stoi(row[0]);
        unsigned long timestamp = std::stol(row[2]);

        timestamps[commit_id] = timestamp;
    }

    std::unordered_map<unsigned int, unsigned long> const & TimestampReader::getTimestamps() {
        return timestamps;
    }
}
