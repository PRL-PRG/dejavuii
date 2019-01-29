#pragma once

#include <string>

namespace dejavu {


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
 * The algorithm has two steps and step one does two things simultaneously. Step 1 constructs auxiliary structures,
 * step 2 uses these structures to construct a graph that is created by removing all not selected nodes from the
 * initial graph, but rewriting edges, so that they follow causality between commits.
 *
 * Step 1a
 *
 * In the first step a target substitution index is created. This is a map of the following form:
 *
 *      targetSubstitutionIndex: Map<Hash, List<Hash>>
 *
 * The Incidence list is scanned. If the source commit is **not selected** then it is added to the target substitution
 * index: the commit is the key and the list of parents is the value.
 *
 * Step 1b
 *
 * Simultaneously, during the first scan also create an edge list of the form:
 *
 *      Edge(source: Commit, target: Commit)
 *
 *      edgeList: List(Edge)
 *
 * When the Incidence list is scanned, if the source commit C is **selected** then it is unpacked into the edge list, so
 * that for every parent P in the Incidence structure a separate edge is appended to the list whose source is P and
 * whose target is C.
 *
 * Step 2
 *
 * During the second step, the edge list is scanned. For each edge with source S and target T, if S is **not selected**
 * then remove the edge from the list. Then, use the targetSubstitutionIndex to find a list of substitutions for S. For
 * each substitution S', create an edge from S' to T and append it to the edge list.
 *
 * Output
 *
 * The edge list is outputted to a CSV file in the format:
 *
 *      project_id,source_hash,target_hash
 *
 * The CSV file may contain duplicates (even within a single project).
 */

    
} // namespace dejavu
