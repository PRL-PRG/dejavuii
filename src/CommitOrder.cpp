#include "CommitOrder.h"

#include <algorithm>

using namespace dejavu;
using namespace std;

CommitOrder::CommitOrder(const std::string input_path,
                         const std::string output_path,
                         const timestamp_map_t & timestamps)
        : input_path(input_path), 
          output_path(output_path),
          timestamps(timestamps) {} 

void CommitOrder::read() {
    first_row = true;

    ofstream csv_file (output_path, ios::out | ios::trunc);
    csv_file.close();

    parse(input_path);
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

    commits[commit_id] = commit;
    
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
    commit->path_ids.insert(path_id);
}

set<path_id_t> get_common_files(commit_info * a, commit_info * b) {
    set<path_id_t> intersection;    
    set_intersection(a->path_ids.begin(), a->path_ids.end(), 
                     b->path_ids.begin(), b->path_ids.end(), 
                     inserter(intersection, intersection.begin()));
    //return intersection.size() > 0;
    return intersection;
}

void write_out_to_csv(ofstream * csv_file, project_id_t project, 
                      commit_id_t predecessor, commit_id_t successor,
                      set<path_id_t> * common_files) {


    set<path_id_t>::iterator i;
    for (i = common_files->begin(); i != common_files->end(); ++i) {
        path_id_t path_id = *i; 
        (*csv_file) << project << "," 
                    << predecessor << "," 
                    << successor << ","
                    << path_id 
                    << endl;
    }
}

void CommitOrder::process_existing_data() {

    // Open the output file for appending.
    ofstream csv_file;
    csv_file.open(output_path, ios::out | ios::app);

    // Let's count relations.
    int relations = 0;

    // Create the order. Compare all commits within the project amongs each
    // other.
    for (auto & outer : commits) {
        commit_info * out = outer.second;

        for (auto & inner : commits) {
            commit_info * in = inner.second;

            // The commit neither precedes nor succeeds itself, the order
            // relation is not reflexive.
            if (out->commit_id == in->commit_id)
                continue;

            // Check if the commits have any files in common. If they do not,
            // they are not ordered.
            set<path_id_t> common_files = get_common_files(in, out);
            if (common_files.size() == 0)
                continue;

            // Retrieve timestamps from the map gathered earlier.
            timestamp_t in_timestamp = getTimestamp(in->commit_id);
            timestamp_t out_timestamp = getTimestamp(out->commit_id);

            // If the timestamps are the same, there is no order relation. I
            // guess this probably doesn't happen very often, if at all.
            if (in_timestamp == out_timestamp)
                continue;
            
            // If the in timestamp precedes the out timestamp, we create that
            // relationship. We output it to a CSV file.
            if (in_timestamp < out_timestamp) 
                write_out_to_csv(&csv_file, current_project, in->commit_id, 
                                 out->commit_id, &common_files);

            // If the in timestamp follows the out timestamp, we create that
            // relationship. We output it to a CSV file.
            if (in_timestamp > out_timestamp) 
                write_out_to_csv(&csv_file, current_project, out->commit_id, 
                                 in->commit_id, &common_files);

            // Count relations.
            relations += common_files.size();
        }
    }

    csv_file.close();

    cerr << "Written out " << relations << " relations for project #" 
         << current_project << endl;    

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

        cerr << "Processing commits for project #" << current_project << endl;
        process_existing_data();
        current_project = project_id;

        cerr << "Reading project #" << project_id << endl;
    }

    // Process the data from the current row.
    aggregate_project_info(project_id, path_id, commit_id);
}
