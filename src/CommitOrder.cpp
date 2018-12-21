#include "CommitOrder.h"

using namespace dejavu;
using namespace std;

CommitOrder::CommitOrder(const std::string path,
                         const timestamp_map_t & timestamps)
        : path(path), timestamps(timestamps) {}

void CommitOrder::read() {
    first_row = true;
    parse(path);
}

timestamp_t CommitOrder::getTimestamp(commit_id_t commit_id) {

    auto pair = timestamps.find(commit_id);
    if (pair == timestamps.end()) {
        cerr << "[BELGIUM] Did not find commit #" << commit_id
             << " in commits." << endl;
        return 0; // FIXME
    }

    return pair->second;
}

commit_info * CommitOrder::getCommit(commit_id_t commit_id,
                                     project_id_t project_id,
                                     timestamp_t timestamp) {

    auto pair = commits.find(commit_id);
    if (pair != commits.end()) {
        return pair->second;
    }

    commit_info * commit = new commit_info;
    commit->project_id = project_id;
    commit->commit_id = commit_id;
    commit->timestamp = timestamp;
    return commit;
}

void CommitOrder::aggregate_project_info(project_id_t project_id,
                                         path_id_t path_id,
                                         commit_id_t commit_id) {
    // First, we find the commit associated with this particular row.
    timestamp_t timestamp = getTimestamp(commit_id);

    // Second, we check if we already have an aggregated commit associated
    // with this commit id. If we do not we need to create a new
    // aggregated_commit entry. Otherwise, we grab a reference to it.
    commit_info * commit = getCommit(commit_id, project_id, timestamp);

    // Third, we add the file into the aggregated commit's file list.
    commit->path_ids.push_back(path_id);
}

void CommitOrder::process_existing_data() {
    // Create the order.

    // Finally, remove all data aggregated so far.
    for (auto & iterator : commits) {
        free(iterator.second);
    }
    commits.clear();
}

void CommitOrder::row(std::vector<std::string> & row){

    // Get the basic data from the row, convert into appropriate types.
    project_id_t project_id = atoi(row[0].c_str());
    path_id_t path_id = atoi(row[1].c_str());
    //hash_id_t hash_id = atoi(row[2].c_str());
    commit_id_t commit_id = atoi(row[3].c_str());

    // If this is the first row, initialize the current project field.
    if (first_row) {
        current_project = project_id;
        first_row = false;

        cerr << "Reading project #" << project_id << endl;
    }

    // If the project changed, process all the data collected so far and
    // initialize a new project.
    if (project_id != current_project) {
        cerr << "Done reading project #" << current_project << endl;
        cerr << "Read " << commits.size() << " commits" << endl;

        process_existing_data();
        current_project = project_id;

        cerr << "Reading project #" << project_id << endl;
    }

    // Process the data from the current row.
    aggregate_project_info(project_id, path_id, commit_id);
}