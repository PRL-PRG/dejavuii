#include <unordered_map>
#include "TimestampReader.h"

using namespace std;
using namespace dejavu;

TimestampReader::TimestampReader(const std::string path) : path(path) {}

void TimestampReader::read() {
    parse(path);
}

void TimestampReader::row(vector<string> & row) {
    commit_id_t commit_id = atoi(row[0].c_str());
    timestamp_t timestamp = atol(row[1].c_str());
    timestamps[commit_id] = timestamp;
}

const timestamp_map_t & TimestampReader::getTimestamps() {
    return timestamps;
}