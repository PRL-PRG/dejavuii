#include <iostream>
#include <vector>
#include <unordered_map>
#include <ctime>

#include "../loaders.h"
#include "../commands.h"
#include "../../helpers/strings.h"

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

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;

            static Project * Create(unsigned id, uint64_t createdAt) {
                Project * result = new Project(id, createdAt);
                if (id >= Project::Projects_.size())
                    Project::Projects_.resize(id + 1);
                Project::Projects_[id] = result;
                return result;
            }

            static Project * Get(unsigned id) {
                assert(id < Project::Projects_.size());
                return Project::Projects_[id];
            }

            static std::vector<Project *> const & GetAll() {
                return Project::Projects_;
            }

        private:
            Project(unsigned id, uint64_t createdAt):
                    id(id),
                    createdAt(id) {
            }
            static std::vector<Project *> Projects_;
        };

        std::vector<Project *> Project::Projects_;

        class ModificationCluster {
        public:
            ModificationCluster(std::vector<Modification *> modifications) : modifications(modifications) {
                // Elect first modification as provisionally oldest.
                Modification *oldest = modifications[0];
                oldest->timestamp = Commit::GetTimestamp(oldest->commit_id);

                // Select oldest modification (from oldest project in case of a tie).
                for (int i = 1, size = modifications.size(); i < size; i++) {
                    modifications[i]->timestamp = Commit::GetTimestamp(modifications[i]->commit_id);

                    if (modifications[i]->timestamp < oldest->timestamp) {
                        oldest = modifications[i];
                        continue;
                    }

                    if (modifications[i]->timestamp == oldest->timestamp) {
                        Project *current_project = Project::Get(modifications[i]->project_id);
                        Project *oldest_project = Project::Get(oldest->project_id);

                        assert(current_project->createdAt != 0);
                        assert(oldest_project->createdAt != 0);

                        if (current_project->createdAt < oldest_project->createdAt) {
                            oldest = modifications[i];
                        }
                        continue;
                    }
                }

                // Mark oldest modification (oldest modification from the oldest project).
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
                clock_t begin;
                clock_t end;

                std::cerr << "COWTING REPEATZ OF CONE TENTS" << std::endl;
                begin = clock();
                std::unordered_map<unsigned, unsigned> counters;
                FileChangeLoader([&counters](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) mutable {
                    counters[contents_id]++;
                });
                end = clock();
                std::cerr << "DONE COWTING REPEATZ OF CONE TENTS IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                std::cerr << "CONTING (PLURAL) CONTENT KLUSTERS" << std::endl;
                begin = clock();
                int counter = 0, pluralities = 0;
                for (auto it : counters) {
                    if (it.second > 1) {
                        pluralities++;
                    }
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " << (counter / 1000) << "k\r"
                                  << std::flush;
                    }
                }
                std::cerr << counter << " items examined" << std::endl;
                end = clock();
                std::cerr << "DER " << pluralities << " (PLURAL) CKONTENT CKLUSTERZ IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                std::cerr << "KOLLECTINK MODIFIKATIONZ FOR KONTENT KLUSTERZ" << std::endl;
                begin = clock();
                std::unordered_map<unsigned, std::vector<Modification*>> clusters;
                int skipped=0;
                pluralities=0;
                FileChangeLoader([&](unsigned project_id,
                                     unsigned commit_id,
                                     unsigned path_id,
                                     unsigned contents_id) mutable {
                    if (counters[contents_id] < 2) {
                        skipped++;
                        return;
                    }
                    clusters[contents_id].push_back(new Modification(project_id,
                                                                     commit_id,
                                                                     path_id,
                                                                     contents_id));
                    pluralities++;
                });
                end = clock();
                std::cerr << "DONE KOLLECTINK MODIFIKATIONZ FOR KONTENT KLUSTERZ (skipped=" << skipped << "clusters=" << pluralities << ") IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                std::string filename = DataDir.value() + "/fileClones.csv";
                std::cerr << "SAVING MODIFIKATION LIST TO FILE AT " << filename << std::endl;
                begin = clock();
                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }
                counter = 0;
                s << "projectId,commitId,pathId,numFiles,contentId/cloneId" << std::endl;
                for (auto & it : clusters) {
                    std::vector<Modification*>& modifications = it.second;
                    for (Modification* modification : modifications) {
                        s << modification->project_id << ","
                          << modification->commit_id << ","
                          << modification->path_id << ","
                          << 1 << ","
                          << modification->contents_id << std::endl;
                        counter++;
                        if (counter % 1000 == 0) {
                            std::cerr << " : " <<(counter / 1000) << "k\r" << std::flush;
                        }
                    }
                }
                std::cerr << counter << " modifications written" << std::endl;
                s.close();
                end = clock();
                std::cerr << "DONE SAVING MODIFIKATION LIST TO FILE IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                std::cerr << "LOADING PROJEKT CREATION DATEZ " << std::endl;
                begin = clock();
                ProjectLoader{[](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                    Project::Create(id, createdAt);
                }};
                end = clock();
                std::cerr << "DONE LOADING PROJEKT CREATION DATEZ IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                std::cerr << "MARKING MODIFIKATIONZ IN KLUSTERZ DAT ARE OLDEST IN OLDEST PROJECTZ (" << clusters.size() << ")" << std::endl;
                begin = clock();
                /*int*/ counter = 0;
                for (auto & it : clusters) {
                    // Mark oldest modification in cluster;
                    ModificationCluster *cluster = new ModificationCluster(it.second);
                    ModificationCluster::clusters[it.first] = cluster;

                    // Count processed lines
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " <<(counter / 1000) << "k\r" << std::flush;
                    }
                }
                std::cerr << counter << " clusters marked" << std::endl;
                end = clock();
                std::cerr << "DONE MARKING MODIFIKATIONZ IN KLUSTERZ DAT ARE OLDEST IN OLDEST PROJECTZ IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;

                filename = DataDir.value() + "/fileCloneOriginals.csv";
                std::cerr << "SAVING CLONE ORIGINAL LIST TO FILE AT " << filename << std::endl;
                begin = clock();
                std::ofstream so(filename);
                if (! so.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }
                counter = 0;
                so << "contentId/cloneId,numFiles,projectId,commitId,pathId" << std::endl;
                for (auto & it : ModificationCluster::clusters) {
                    ModificationCluster *cluster = it.second;
                    Modification *original = cluster->original;
                    so << original->contents_id << ","
                      << 1 << ","
                      << original->project_id << ","
                      << original->commit_id << ","
                      << original->path_id << ",";
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " <<(counter / 1000) << "k\r" << std::flush;
                    }
                }
                std::cerr << counter << " clone originals written" << std::endl;
                so.close();
                end = clock();
                std::cerr << "DONE SAVING CLONE ORIGINAL LIST TO FILE IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;
            }

            static void SaveClusters() {
                std::cerr << "WRITINK OUT KLUSTER INFORMESHON" << std::endl;
                clock_t begin = clock();
                const std::string filename = DataDir.value() + "/fileClusters.csv";
                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "\"content id\",\"cluster size\",\"original commit id\""
                  << std::endl;

                int counter = 0;
                for (auto & it : clusters) {
                    unsigned content_id = it.first;
                    unsigned cluster_size = it.second->size();
                    unsigned original = it.second->get_original()->commit_id;
                    s << content_id << "," << cluster_size << "," << original
                      << std::endl;

                    // Count processed lines
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " <<(counter / 1000) << "k\r" << std::flush;
                    }
                }
                std::cerr << counter << " lines written" << std::endl;
                clock_t end = clock();
                std::cerr << "DONE WRITINK OUT KLUSTER INFORMESHON IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;
            }

            static void SaveClusterCommits() {
                std::cerr << "WRITINK OUT KLUSTER COMMIT INFORMESHON" << std::endl;
                clock_t begin = clock();
                const std::string filename = DataDir.value() + "/fileClustersWithCommits.csv";
                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "\"content id\",\"cluster size\",\"original commit id\",\"commit list\""
                  << std::endl;

                int counter = 0;
                for (auto & it : clusters) {
                    unsigned content_id = it.first;
                    unsigned cluster_size = it.second->size();
                    unsigned original = it.second->get_original()->commit_id;

                    s << content_id << "," << cluster_size << "," << original << ",";
                    bool first = true;
                    for (auto & mod : it.second->modifications) {
                        if (first) {
                            first = false;
                        } else {
                            s << " ";
                        }
                        s << mod->commit_id;
                    }
                    s << std::endl;

                    // Count processed lines
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " <<(counter / 1000) << "k\r" << std::flush;
                    }
                }
                s.close();
                std::cerr << counter << " lines written" << std::endl;
                clock_t end = clock();
                std::cerr << "DONE WRITINK OUT KLUSTER COMMIT INFORMESHON IN " << (double(end - begin) / CLOCKS_PER_SEC) << "s" << std::endl;
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

        std::cerr << "LOAD TIMESTAMPZ" << std::endl;
        Commit::LoadTimestamps();
        std::cerr << "DONE LOAD TIMESTAMPZ" << std::endl;

        ModificationCluster::LoadClusters();
        ModificationCluster::SaveClusters();
        ModificationCluster::SaveClusterCommits();
    }
    
} // namespace dejavu
