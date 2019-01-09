#include <unordered_map>
#include "timestamp-reader.h"

using namespace std;
using namespace dejavu;

TimestampReader::TimestampReader(const std::string path) : path(path) {}

void TimestampReader::read() {
    parse(path);
}

void TimestampReader::row(vector<string> & row) {
    unsigned int commit_id = atoi(row[0].c_str());
    unsigned long timestamp = atol(row[1].c_str());
    timestamps[commit_id] = timestamp;
}

const std::unordered_map<unsigned int, unsigned long> & TimestampReader::getTimestamps() {
    return timestamps;
}