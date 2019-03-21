#pragma once

#include <string>

/**
 * Input data
 *
 * The input data is a file containing the definition for a commit graph for each surveyed project. Each surveyed
 * project is a separate section delimited by a line starting with # and the ID of the project. Then each additional
 * line describes a commit within that project. The number of commits can be 0. Each commit line is in the following
 * format:
 *
 *      commit_description := commit_hash, " ", commit_time, " ", author_time, "--", commit_tag
 *                          | commit_hash, " ", commit_time, " ", author_time, " ", parent_list, "--", commit_tag
 *                          ;
 *
 *             parent_list := commit_hash
 *                          | commit_hash, " ", parent_list
 *                          ;
 *
 * Note that commit's own hash and the hashes in the parent list create an incidence list that defines the commit graph.
 *
 * Data is loaded into memory and processed one project at a time. For each project we generate a list of Incidence
 * structures:
 *
 *      Incidence(hash: Hash, parents: List<Hash>)
 *
 * Commit selection criteria
 *
 * Commit selection shall be a simple set of commit hashes. The information whether a hash is a member of the selection
 * set will also be encoded along with the hash of each commit as follows:
 *
 *      Commit(hash: Hash, selected: Boolean)
 *      Incidence(commit: Commit, parents: List<Commit>)
 *
 * Algorithms
 *
 * TODO
 *
 * Output
 *
 * The edge list is outputted to a CSV file in the format:
 *
 *      project_id,source_hash,target_hash
 *
 * The CSV file may contain duplicates (even within a single project).
 */

namespace dejavu {

    extern helpers::Settings settings;
    void SelectiveCommitNetwork(int argc, char * argv[]);

} // namespace dejavu
