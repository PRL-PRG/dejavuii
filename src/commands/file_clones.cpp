#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {
        class Commit {
        public:
            static void LoadTimestamps() {
                CommitLoader([](unsigned id, uint64_t author_time, uint64_t commit_time) {
                    assert(timestamps.find(id) == timestamps.end());
                    timestamps[id] = author_time;
                });
            }
            static uint64_t GetTimestamp(unsigned commit_id) {
                assert(timestamps.find(commit_id) != timestamps.end());
                return timestamps[commit_id];
            }
        private:
            static std::unordered_map<unsigned, uint64_t> timestamps;
        };

        std::unordered_map<unsigned, uint64_t> Commit::timestamps;

        class Modification {
        public:
            Modification(unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) :
                    project_id(project_id), commit_id(commit_id), path_id(path_id), contents_id(contents_id) {};

        protected:
            const unsigned project_id;
            const unsigned commit_id;
            const unsigned path_id;
            const unsigned contents_id;
            u_int64_t timestamp = 0L;
            bool original = false;

            friend class ModificationCluster;
        };

        class ModificationCluster {
        public:
            static void LoadClusters() {
                std::unordered_map<unsigned, std::vector<Modification *>> possible_clusters;
                FileChangeLoader::RowHandler handler = [possible_clusters](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) mutable {
                    Modification *modification = new Modification(project_id, commit_id, path_id, contents_id);
                    possible_clusters[contents_id].push_back(modification);
                };
                FileChangeLoader loader = FileChangeLoader(handler);

                for (auto & it : possible_clusters) {
                    // Only preserve those clusters that have more than one
                    // modification.
                    if (it.second.size() < 2) {
                        for (auto modification : it.second) {
                            delete modification;
                        }
                        continue;
                    }

                    // Copy the cluster to the actual output data structure.
                    clusters[it.first] = it.second;

                    // Mark oldest modification in cluster;
                    std::vector<Modification *> &modifications = clusters[it.first];

                    // Elect first modification as provisionally oldest.
                    Modification *oldest = modifications[0];
                    oldest->timestamp = Commit::GetTimestamp(oldest->commit_id);


                    // Select oldest modification.
                    for (int i = 1, size = modifications.size(); i < size; i++) {
                        if (modifications[i]->timestamp < oldest->timestamp) {
                            modifications[i]->timestamp = Commit::GetTimestamp(modifications[i]->commit_id);
                            oldest = modifications[i];
                        }
                    }

                    // Mark oldest modification.
                    oldest->original = true;
                }
            }
        protected:
            static std::unordered_map<unsigned, std::vector<Modification *>> clusters;
        };

        std::unordered_map<unsigned, std::vector<Modification *>> ModificationCluster::clusters;
    } //anonymoose namespace

    void DetectFileClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        //Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Commit::LoadTimestamps();
        ModificationCluster::LoadClusters();
    }
    
} // namespace dejavu
