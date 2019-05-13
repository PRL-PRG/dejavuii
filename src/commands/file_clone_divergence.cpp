#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Commit {
        public:
            unsigned id;
            uint64_t author_time;
            uint64_t committer_time;
            unsigned n_parents;
            std::vector<Commit *> children;
            std::unordered_map<unsigned, unsigned> changes; /* path -> content? */
            std::unordered_set<unsigned> projects;

            Commit(unsigned id, uint64_t author_time, uint64_t committer_time):
                    id(id), author_time(author_time),
                    committer_time(committer_time),
                    n_parents(0) {}

            // obligatory for ForwardCommitIterator
            unsigned numParentCommits() {
                return n_parents;
            }

            // obligatory for ForwardCommitIterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }
        };

        class FileCluster {
        public:
            unsigned content_id;
            unsigned cluster_size;
            unsigned original_commit_id;
            std::vector<unsigned> commits;
            FileCluster(unsigned content_id,
                        unsigned original_commit_id,
                        std::vector<unsigned> commits):

                    content_id(content_id),
                    original_commit_id(original_commit_id),
                    commits(commits) {}
        };

        class Aggregation {
        public:

            std::unordered_map<unsigned, unsigned> path_modifications; /*path id -> count */

            Aggregation() {
            }

            Aggregation(Aggregation const & from) {
                mergeWith(from);
            }

            Aggregation(Aggregation &&) = delete;

            Aggregation & operator = (Aggregation const &) = delete;
            Aggregation & operator = (Aggregation &&) = delete;

            ~Aggregation() {
                //delete root_;
            }

            void mergeWith(Aggregation const & from) {
                for (auto pm : from.path_modifications) {
                    path_modifications[pm.first] += pm.second;
                }
            }
        };
    }

    void InspectFileClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        //Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::cerr << "LOAD COMMITZ" << std::endl;
        std::unordered_map<unsigned, Commit*> commits;
        CommitLoader([&commits](unsigned id,
                                uint64_t author_time,
                                uint64_t committer_time) {
            commits[id] = new Commit(id, author_time, committer_time);
        });
        std::cerr << "DONE LOAD COMMITZ" << std::endl;

        std::cerr << "LOAD COMMITZ PARENTZ" << std::endl;
        CommitParentsLoader{[&commits](unsigned id, unsigned parent_id){
            assert(commits.find(id) != commits.end());
            commits[id]->n_parents++;
            commits[parent_id]->children.push_back(commits[id]);
        }};
        std::cerr << "DONE LOAD COMMITZ PARENTZ" << std::endl;

        std::cerr << "LOAD COMMITZ CHANGES" << std::endl;
        FileChangeLoader{[&commits](unsigned project_id,
                                    unsigned commit_id,
                                    unsigned path_id,
                                    unsigned contents_id) {

            assert(commits.find(commit_id) != commits.end());
            assert(commits[commit_id]->changes.find(path_id)
                   == commits[commit_id]->changes.end());

            commits[commit_id]->changes[path_id] = contents_id;
            commits[commit_id]->projects.insert(project_id);

        }};
        std::cerr << "DONE LOAD COMMITZ CHANGES" << std::endl;

        std::cerr << "LOAD FILE CLONE KLUSTERZ" << std::endl;
        //std::unordered_map<unsigned, unsigned> clusters;
        std::vector<FileCluster*> clusters;
        FileClusterLoader([&clusters](unsigned content_id,
                             unsigned cluster_size,
                             unsigned original_commit_id,
                             std::vector<unsigned> & commits) {
            //clusters[content_id] = original_commit_id;
            clusters.push_back(new FileCluster(content_id, original_commit_id, commits));
        });
        std::cerr << "DONE LOAD FILE CLONE KLUSTERZ" << std::endl;

        std::cerr << "ANALYZE KLUSTERZ" << std::endl;
        int counter = 0;

        std::unordered_map<unsigned /*content_id*/,
        std::unordered_map<unsigned /*commit_id*/,
        std::unordered_map<unsigned /*project_id*/,
        std::unordered_set<unsigned /*commits ids in which mods occur*/>>>> modifications;

        for (auto cluster : clusters) {
            unsigned content_id = cluster->content_id;
            for (unsigned commit_id : cluster->commits) {
                CommitForwardIterator<Commit, Aggregation> cfi([&modifications,content_id,commit_id](Commit *commit, Aggregation &aggregation) -> bool {
                    for (unsigned project_id : commit->projects) {
                        if (commit->id == commit_id) {
                            // Ignore initial commit.
                            continue;
                        }
                        modifications[content_id][commit_id][project_id].insert(commit->id); // FIXME only commits that modify this path
                    }
                });
                cfi.addInitialCommit(commits[commit_id]);
                cfi.process();
            }
            counter++;
            if (counter % 1000 == 0) {
                std::cerr << " : " << (counter / 1000) << "k\r" << std::flush;
            }
        }
        std::cerr << "DONE ANALYZE KLUSTERZ" << std::endl;

        std::cerr << "SAVE ANALYSAUCE" << std::endl;
        std::cerr << "content_id" << ","
                  << "root_commit_id" << ","
                  << "project_id" << ","
                  << commits.size() << std::endl;
        for (auto & content : modifications) {
            unsigned content_id = content.first;
            for (auto & root_commit : content.second) {
                unsigned root_commit_id = root_commit.first;
                for (auto & project : root_commit.second) {
                    unsigned project_id = project.first;
                    std::unordered_set modifying_commits = project.second;

                    std::cerr << content_id << ","
                              << root_commit_id << ","
                              << project_id << ","
                              << modifying_commits.size() << std::endl;
                }
            }
        }
        std::cerr << "DONE SAVE ANALYSAUCE" << std::endl;
    }
    
} // namespace dejavu
