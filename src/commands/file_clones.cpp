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
            ModificationCluster(std::vector<Modification *> modifications) : modifications(modifications) {
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
                original = oldest;
            }

            size_t size() {
                return modifications.size();
            }

            Modification* get_original() {
                return original;
            }

            static void LoadClusters() {
                std::cerr << "COWTING REPEATZ OF CONE TENTS" << std::endl;
                std::unordered_map<unsigned, unsigned> counters;
                FileChangeLoader([counters](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) mutable {
                    counters[contents_id]++;
                });
                std::cerr << "DONE COWTING REPEATZ OF CONE TENTS" << std::endl;

                std::cerr << "KOLLECTINK MODIFIKATIONZ FOR KONTENT KLUSTERZ" << std::endl;
                std::unordered_map<unsigned, std::vector<Modification*>> clusters;
                FileChangeLoader([counters,clusters](unsigned project_id,
                                                     unsigned commit_id,
                                                     unsigned path_id,
                                                     unsigned contents_id) mutable {
                    if (counters[commit_id] < 2)
                        return;
                    clusters[contents_id].push_back(new Modification(project_id,
                                                                     commit_id,
                                                                     path_id,
                                                                     contents_id));
                });
                std::cerr << "DONE KOLLECTINK MODIFIKATIONZ FOR KONTENT KLUSTERZ" << std::endl;

                std::cerr << "MARKING OLDEST MODIFIKATIONZ IN KLUSTERZ" << std::endl;
                for (auto & it : clusters) {
                    // Mark oldest modification in cluster;
                    ModificationCluster *cluster = new ModificationCluster(it.second);
                    ModificationCluster::clusters[it.first] = cluster;
                }
                std::cerr << "DONE MARKING OLDEST MODIFIKATIONZ IN KLUSTERZ" << std::endl;
            }

            static void SaveClusters() {
                std::cerr << "WRITINK OUT KLUSTER INFORMESHON" << std::endl;
                const std::string filename = DataDir.value() + "/fileClusters.csv";
                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "\"content id\",\"cluster size\",\"original commit id\""
                  << std::endl;

                for (auto & it : clusters) {
                    unsigned content_id = it.first;
                    unsigned cluster_size = it.second->size();
                    unsigned original = it.second->get_original()->commit_id;
                    s << content_id << "," << cluster_size << "," << original
                      << std::endl;
                }

                std::cerr << "DONE WRITINK OUT KLUSTER INFORMESHON" << std::endl;
            }

        protected:
            std::vector<Modification *> modifications;
            Modification * original;

            static std::unordered_map<unsigned, ModificationCluster*> clusters;
        };

        std::unordered_map<unsigned, ModificationCluster*> ModificationCluster::clusters;
    } //anonymoose namespace



    void DetectFileClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        //Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Commit::LoadTimestamps();
        ModificationCluster::LoadClusters();
        ModificationCluster::SaveClusters();
    }
    
} // namespace dejavu
