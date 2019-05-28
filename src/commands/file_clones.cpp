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
                auto iterator = modifications.begin();
                Modification *oldest = *iterator; ++iterator;
                oldest->timestamp = Commit::GetTimestamp(oldest->commit_id);

                // Select oldest modification (from oldest project in case of a tie).
                for (auto end = modifications.end(); iterator != end; ++iterator) {
                    Modification *modification = *iterator;
                    modification->timestamp = Commit::GetTimestamp(modification->commit_id);

                    if (modification->timestamp < oldest->timestamp) {
                        oldest = modification;
                        continue;
                    }

                    if (modification->timestamp == oldest->timestamp) {
                        Project *current_project = Project::Get(modification->project_id);
                        Project *oldest_project = Project::Get(oldest->project_id);

                        assert(current_project->createdAt != 0);
                        assert(oldest_project->createdAt != 0);

                        if (current_project->createdAt < oldest_project->createdAt) {
                            oldest = modification;
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

            static void CountRepeatsOfContents(std::unordered_map<unsigned, unsigned> &counters){
                clock_t timer;
                std::string task = "counting repeats of contents";
                helpers::StartTask(task, timer);

                FileChangeLoader([&counters](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) mutable {
                    counters[contents_id]++;
                });

                helpers::FinishTask(task, timer);
            }

            static void CountPluralContentClusters(std::unordered_map<unsigned, unsigned> const &counters) {
                clock_t timer;
                std::string task = "counting plural content clusters";
                helpers::StartTask(task, timer);

                unsigned counter;
                helpers::StartCounting(counter);

                unsigned pluralities = 0;

                for (auto it : counters) {
                    if (it.second > 1) {
                        ++pluralities;
                    }
                    helpers::Count(counter);
                }

                helpers::FinishCounting(counter, "clusters (singular and plural)");
                std::cerr << "found " << pluralities << " plural clusters" << std::endl;
                helpers::FinishTask(task, timer);
            }

            static void CollectModificationsForContentClusters(std::unordered_map<unsigned, unsigned> const &counters,
                                                               std::unordered_map<unsigned, std::vector<Modification*>> &clusters) {

                clock_t timer;
                std::string task = "populating content clusters with modifications";
                helpers::StartTask(task, timer);

                unsigned skipped=0, pluralities=0;

                FileChangeLoader([&](unsigned project_id,
                                     unsigned commit_id,
                                     unsigned path_id,
                                     unsigned contents_id) mutable {
                    if (counters.at(contents_id) < 2) {
                        skipped++;
                        return;
                    }
                    clusters[contents_id].push_back(new Modification(project_id,
                                                                     commit_id,
                                                                     path_id,
                                                                     contents_id));
                    pluralities++;
                });

                std::cerr << "processed " << pluralities << " plural clusters" << std::endl;
                std::cerr << "skipped " << skipped << " singular clusters" << std::endl;

                helpers::FinishTask(task, timer);
            }

            static void SaveModifications(std::unordered_map<unsigned, std::vector<Modification*>> const &clusters) {
                std::string filename = DataDir.value() + "/fileClones.csv";

                clock_t timer;
                std::string task = "saving modifications to " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                unsigned counter;
                helpers::StartCounting(counter);

                s << "projectId" << ","
                  << "commitId" << ","
                  << "pathId" << ","
                  << "numFiles" << ","
                  << "contentId/cloneId" << std::endl;

                for (auto & it : clusters) {
                    std::vector<Modification*> const &modifications = it.second;
                    for (Modification* modification : modifications) {
                        s << modification->project_id << ","
                          << modification->commit_id << ","
                          << modification->path_id << ","
                          << 1 << ","
                          << modification->contents_id << std::endl;

                        helpers::Count(counter);
                    }
                }
                s.close();

                helpers::FinishCounting(counter, "modifications");
                helpers::FinishTask(task, timer);
            }

            static void LoadProjectCreationDates() {
                clock_t timer;
                std::string task = "loading project creation dates";
                helpers::StartTask(task, timer);

                ProjectLoader{
                        [](unsigned id, std::string const &user, std::string const &repo,
                           uint64_t createdAt) {
                            Project::Create(id, createdAt);
                        }};

                helpers::FinishTask(task, timer);
            }

            static void MarkOriginalClonesInClusters(std::unordered_map<unsigned, std::vector<Modification*>> const &clusters) {
                clock_t timer;
                std::string task = "marking original clones in clusters";
                helpers::StartTask(task, timer);

                unsigned counter;
                helpers::StartCounting(counter);

                for (auto &it : clusters) {
                    // Mark oldest modification in cluster;
                    ModificationCluster *cluster = new ModificationCluster(it.second);
                    ModificationCluster::clusters[it.first] = cluster;

                    // Count processed lines
                    helpers::Count(counter);
                }

                helpers::FinishCounting(counter, "clusters");
                helpers::FinishTask(task, timer);
            }

            static void SaveOriginalClones() {
                std::string filename = DataDir.value() + "/fileCloneOriginals.csv";

                clock_t timer;
                std::string task = "saving original clones to " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "contentId/cloneId" << ","
                  << "numFiles" << ","
                  << "projectId" << ","
                  << "commitId" << ","
                  << "pathId" << std::endl;

                unsigned counter = 0;
                helpers::StartCounting(counter);

                for (auto & it : ModificationCluster::clusters) {
                    ModificationCluster *cluster = it.second;
                    Modification *original = cluster->original;
                    s << original->contents_id << ","
                      << 1 << ","
                      << original->project_id << ","
                      << original->commit_id << ","
                      << original->path_id << ",";

                    helpers::Count(counter);
                }

                s.close();

                helpers::FinishCounting(counter, "clone clusters");
                helpers::FinishTask(task, timer);
            }

            static void SaveClusters() {
                const std::string filename = DataDir.value() + "/fileClusters.csv";

                clock_t timer;
                std::string task = "saving clusters " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);

                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "contentId" << ","
                  << "clusterSize" << ","
                  << "originalCommitId" << std::endl;

                unsigned  counter;
                helpers::StartCounting(counter);

                for (auto & it : clusters) {
                    unsigned content_id = it.first;
                    unsigned cluster_size = it.second->size();
                    unsigned original = it.second->get_original()->commit_id;

                    s << content_id << ","
                      << cluster_size << ","
                      << original << std::endl;

                    helpers::Count(counter);
                }

                s.close();

                helpers::FinishCounting(counter, "clone clusters");
                helpers::FinishTask(task, timer);
            }

            static void SaveClusterCommits() {
                const std::string filename = DataDir.value() + "/fileClustersWithCommits.csv";

                clock_t timer;
                std::string task = "saving clusters (with commit information) to " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "contentId" << ","
                  << "clusterSize" << ","
                  << "originalCommitId" << ","
                  << "commitList" << std::endl;

                unsigned counter;
                helpers::StartCounting(counter);

                for (auto & it : clusters) {
                    unsigned content_id = it.first;
                    unsigned cluster_size = it.second->size();
                    unsigned original = it.second->get_original()->commit_id;

                    s << content_id << ","
                      << cluster_size << ","
                      << original << ",";

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

                    helpers::Count(counter);
                }

                s.close();

                helpers::FinishCounting(counter, "clone clusters");
                helpers::FinishTask(task, timer);
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

        std::unordered_map<unsigned, unsigned> counters;
        ModificationCluster::CountRepeatsOfContents(counters);

        ModificationCluster::CountPluralContentClusters(counters);

        std::unordered_map<unsigned, std::vector<Modification*>> clusters;
        ModificationCluster::CollectModificationsForContentClusters(counters,
                                                                    clusters);

        ModificationCluster::SaveModifications(clusters);
        ModificationCluster::LoadProjectCreationDates();
        ModificationCluster::MarkOriginalClonesInClusters(clusters);

        ModificationCluster::SaveOriginalClones();
        ModificationCluster::SaveClusters();
        ModificationCluster::SaveClusterCommits();
    }
    
} // namespace dejavu
