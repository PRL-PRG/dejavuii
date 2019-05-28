#include <iostream>
#include <vector>
#include <unordered_map>
#include <ctime>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Commit {
        public:
            unsigned id;
//            uint64_t author_time;
//            uint64_t committer_time;
            unsigned n_parents;
            std::vector<Commit *> children;
//            std::unordered_map<unsigned, unsigned> changes; /* path -> content */
//            std::unordered_set<unsigned> projects;

            Commit(unsigned id): id(id), n_parents(0) {}

//            Commit(unsigned id, uint64_t author_time, uint64_t committer_time):
//                    id(id), author_time(author_time),
//                    committer_time(committer_time),
//                    n_parents(0) {}

            // obligatory for ForwardCommitIterator
            unsigned numParentCommits() {
                return n_parents;
            }

            // obligatory for ForwardCommitIterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }
        };

//        class ProjectAlignedCommit {
//        public:
//            static ProjectAlignedCommit* Find(Commit* commit, unsigned project) {
//
//            }
//            static ProjectAlignedCommit* Register(Commit* commit, unsigned project) {
//
//            }
//
//            Commit* commit;
//            unsigned project;
//            std::vector<ProjectAlignedCommit*> children;
//
//            ProjectAlignedCommit(unsigned project, Commit* commit): project(project), commit(commit) {
//                assert(commit != nullptr);
//                for (Commit * child : commit->childrenCommits()) {
//                    ProjectAlignedCommit* possible = ProjectAlignedCommit::Find(child, project);
//                    if(possible != nullptr) {
//                        children.push_back(possible);
//                    }
//            }
//
//            unsigned numParentCommits() {
//                return commit->numParentCommits();
//            }
//
//            std::vector<ProjectAlignedCommit*> const & childrenCommits() const {
//                return children;
//            }
//        };

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

        class DummyState {
        public:

            DummyState() {
            }

            DummyState(DummyState const & from) {
                mergeWith(from);
            }

            DummyState(DummyState &&) = delete;

            DummyState & operator = (DummyState const &) = delete;
            DummyState & operator = (DummyState &&) = delete;

            ~DummyState() {
                //delete root_;
            }

            void mergeWith(DummyState const & from) {

            }
        };

//        class TrackingState {
//        protected:
//            std::unordered_set<std::pair<unsigned, unsigned>> tracked;
//            std::unordered_map<std::pair<unsigned, unsigned>, unsigned> boops;
//            std::unordered_map<std::pair<unsigned, unsigned>, unsigned> source_commit;
//            std::unordered_map<std::pair<unsigned, unsigned>, unsigned> source_content;
//
//        public:
//
//            TrackingState() {
//            }
//
//            TrackingState(TrackingState const & from) {
//                mergeWith(from);
//            }
//
//            TrackingState(TrackingState &&) = delete;
//
//            TrackingState & operator = (TrackingState const &) = delete;
//            TrackingState & operator = (TrackingState &&) = delete;
//
//            ~TrackingState() {
//                //delete root_;
//            }
//
//            void mergeWith(TrackingState const & from) {
//                for (std::pair<unsigned, unsigned> key : from.tracked) {
//                    tracked.insert(key);
//                }
//
//                for (auto entry : from.boops) {
//                    std::pair<unsigned, unsigned> key = entry.first;
//                    unsigned count = entry.second;
//                    boops[key] = std::max(count, boops[key]);
//                }
//            }
//
//            void track(unsigned content_id, unsigned commit_id, unsigned project_id, unsigned path_id) {
//                std::pair<unsigned, unsigned> key = std::make_pair(project_id, path_id);
//
//                assert(source_commit.find(key) == source_commit.end());
//                assert(source_content.find(key) == source_content.end());
//
//                tracked.insert(key);
//                source_commit[key] = commit_id;
//                source_content[key] = content_id;
//            }
//
//            bool isTracked(unsigned project_id, unsigned path_id) {
//                return tracked.find(std::make_pair(project_id, path_id)) != tracked.end();
//            };
//
//            void boop(unsigned project_id, unsigned path_id) {
//                boops[std::make_pair(project_id, path_id)]++;
//            };
//
//            void stopTracking(unsigned project_id, unsigned path_id) {
//                tracked.erase(std::make_pair(project_id, path_id));
//            };
//        };
    };

    inline void StartTask(const std::string &task, clock_t &timer) {
        std::cerr << "started " << task << std::endl;
    }

    inline void FinishTask(const std::string task, clock_t &timer) {
        clock_t end = clock();
        std::cerr << "finished " << task
                  << " in " << (double(end - timer) / CLOCKS_PER_SEC) << "s"
                  << std::endl;
    }

    inline void StartCounting(unsigned &counter) {
        counter = 0;
    }

    inline void Count(unsigned &counter) {
        ++counter;
        if (counter % 1000 == 0) {
            std::cerr << " : " << (counter / 1000) << "k\r" << std::flush;
        }
    }

    inline void FinishCounting(unsigned &counter) {
        std::cerr << "iterated over " << counter << " items" << std::endl;
    }

    inline void FinishCounting(unsigned &counter, std::string items_name) {
        std::cerr << "iterated over " << counter << " " << items_name
                  << std::endl;
    }

    void LoadCommitParents(std::unordered_map<unsigned, std::unordered_set<unsigned>> &parents) {
        clock_t timer;
        std::string task = "loading commit parents";
        StartTask(task, timer);

        CommitParentsLoader{[&](unsigned id, unsigned parent_id) {
            assert(parents[id].find(parent_id) == parents[id].end());
            parents[id].insert(parent_id);
        }};

        FinishTask(task, timer);
    }

    void LoadCommitChanges(std::unordered_map<unsigned, std::unordered_set<unsigned>> &project_commits,
                           std::unordered_map<unsigned, std::unordered_set<unsigned>> &commit_projects,
                           std::unordered_map<unsigned, std::unordered_map<unsigned, unsigned>> &commit_changes) {
        clock_t timer;
        std::string task = "loading commit changes";
        StartTask(task, timer);

        FileChangeLoader{[&](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) {

            assert(commit_changes[commit_id].find(path_id) == commit_changes[commit_id].end()
                   || (commit_changes[commit_id].find(path_id))->second == contents_id);

            project_commits[project_id].insert(commit_id);
            commit_projects[commit_id].insert(project_id);
            commit_changes[commit_id][path_id] = contents_id;
        }};

        FinishTask(task, timer);
    }

    void LoadFileCloneClusters(std::vector<FileCluster *> &clusters) {
        clock_t timer;
        std::string task = "loading file clone clusters";
        StartTask(task, timer);

        FileClusterLoader([&clusters](unsigned content_id,
                                      unsigned cluster_size,
                                      unsigned original_commit_id,
                                      std::vector<unsigned> &commits) {

            clusters.push_back(new FileCluster(content_id,
                                               original_commit_id,
                                               commits));
        });

        FinishTask(task, timer);
    }

    void ExtractProjectsContainingClones(std::vector<FileCluster *> const &clusters,
                                         std::unordered_map<unsigned, std::unordered_set<unsigned>> const &commit_projects,
                                         std::unordered_set<unsigned> &projects_containing_clones) {
        clock_t timer;
        std::string task = "extracting projects containing clones";
        StartTask(task, timer);

        unsigned cluster_counter;
        StartCounting(cluster_counter);

        for (FileCluster *cluster : clusters) {
            for (unsigned commit_id : cluster->commits)
                for (unsigned project_id : commit_projects.at(commit_id)) {
                    projects_containing_clones.insert(project_id);
                }
            Count(cluster_counter);
        }

        FinishCounting(cluster_counter, "clusters");

        std::cerr << "found " << projects_containing_clones.size()
                  << " interesting projects";

        FinishTask(task, timer);
    }

    void PruneCommitTreesToProjects(std::unordered_map<unsigned, std::unordered_set<unsigned>> const &commit_parents,
                                    std::unordered_map<unsigned, std::unordered_set<unsigned>> const &project_commits,
                                    std::unordered_map<unsigned, std::unordered_set<unsigned>> const &commit_projects,
                                    std::unordered_set<unsigned> const &projects_containing_clones,
                                    std::unordered_map<unsigned, std::unordered_map<unsigned, Commit *>> &project_commit_trees) {

        clock_t timer;
        std::string task = "pruning commit trees to projects";
        StartTask(task, timer);

        unsigned counter;
        StartCounting(counter);

        for (unsigned project_id : projects_containing_clones) {
            std::unordered_set<unsigned> const &this_project_commits = project_commits.at(project_id);

            // Create a (mostly empty) commit object for each commit in this project.
            for (unsigned commit_id : this_project_commits) {
                project_commit_trees[project_id][commit_id] = new Commit(
                        commit_id);
            }

            // TODO This is kind of horrible
            // Populate commits with their children/parents relations.
            for (unsigned commit_id : this_project_commits) {
                std::unordered_set<unsigned> const &potential_parents = commit_parents.at(commit_id);
                std::unordered_set<unsigned> in_project_parents;
                for (unsigned potential_parent : potential_parents) {
                    if (commit_projects.at(potential_parent).find(project_id) !=
                        commit_projects.at(potential_parent).end()) {
                        in_project_parents.insert(potential_parent);
                        project_commit_trees[project_id][commit_id]->n_parents++;
                        project_commit_trees[project_id][potential_parent]->children.push_back(
                                project_commit_trees[project_id][commit_id]);
                    }
                }
            }

            Count(counter);
        }

        FinishCounting(counter, "projects");
        FinishTask(task, timer);
    }

    void AnalyzeCloneModifications(std::vector<FileCluster *> const &clusters,
                                   std::unordered_map<unsigned, std::unordered_set<unsigned>> const &commit_projects,
                                   std::unordered_map<unsigned, std::unordered_map<unsigned, unsigned>> const &commit_changes,
                                   std::unordered_map<unsigned, std::unordered_map<unsigned, Commit *>> const &project_commit_trees,
                                   std::unordered_map<unsigned, std::unordered_map<unsigned, std::unordered_map<unsigned, std::unordered_map<unsigned, unsigned>>>> modifications) {

        const std::string filename = DataDir.value() + "/fileClonesModificationInstances.csv";

        clock_t timer;
        std::string task = "analyzing clone modifications (traversing project commit graphs for each clone) and writing to " + filename;
        StartTask(task, timer);

        std::ofstream s(filename);

        if (!s.good()) {
            ERROR("Unable to open file " << filename << " for writing");
        }

        s << "contentId" << ","
          << "rootCommitId" << ","
          << "projectId" << ","
          << "pathId" << ","
          << "modificationType" << std::endl;

        unsigned n_clusters = 0;
        unsigned n_traversals = 0;

        for (FileCluster *cluster : clusters) {
            ++n_clusters;
            for (unsigned root_commit_id : cluster->commits) {
                // Figure out which path to track.
                std::unordered_set<unsigned> tracked_paths;
                for (auto &change : commit_changes.at(root_commit_id)) {
                    unsigned changed_path_id = change.first;
                    unsigned changed_content_id = change.second;
                    if (changed_content_id == cluster->content_id) {
                        tracked_paths.insert(changed_path_id);
                    }
                }

                // Figure out which projects to track.
                std::unordered_set<unsigned> const &this_commits_projects = commit_projects.at(root_commit_id);

                // Track the selected path in each of the selected projects.
                for (unsigned project_id : this_commits_projects) {
                    CommitForwardIterator<Commit, DummyState> cfi(
                            [&](Commit *commit, DummyState &_) -> bool {
                                if (commit->id == root_commit_id) {
                                    // Root node, ignore.
                                    return true;
                                }
                                for (auto const & change : commit_changes.at(root_commit_id)) {
                                    unsigned changed_path_id = change.first;
                                    unsigned changed_content_id = change.second;

                                    if (tracked_paths.find(changed_path_id) == tracked_paths.end()) {
                                        continue;
                                    }

                                    if (changed_content_id == 0 /*deleted*/) {

                                        s << cluster->content_id << ","
                                          << root_commit_id << ","
                                          << project_id << ","
                                          << changed_path_id << "," << "D" << std::endl;

                                        return false;
                                    }

                                    s << cluster->content_id << ","
                                      << root_commit_id << ","
                                      << project_id << ","
                                      << changed_path_id << "," << "E" << std::endl;

                                    modifications[cluster->content_id][root_commit_id][project_id][changed_path_id]++;
                                }
                                ++n_traversals;
                                if (n_traversals % 1000 == 0) {
                                    std::cerr << " : "
                                              << (n_clusters / 1000)
                                              << "k clusters "
                                              << (n_traversals / 1000)
                                              << "k traversals\r"
                                              << std::flush;
                                }
                                return true;
                            });
                    cfi.addInitialCommit(project_commit_trees.at(project_id).at(root_commit_id));
                    cfi.process();
                }
            }
        }
        std::cerr << "performed "
                  << n_clusters << "clusters "
                  << n_traversals << "traversals"
                  << std::endl;

        s.close();

        FinishTask(task, timer);
    }

    void WriteOutModifications(std::unordered_map<unsigned, std::unordered_map<unsigned, std::unordered_map<unsigned, std::unordered_map<unsigned, unsigned>>>> const &modifications) {
        const std::string filename = DataDir.value() + "/fileClonesModifications.csv";

        clock_t timer;
        std::string task = "writing out number clone modifications to " + filename;
        StartTask(task, timer);

        unsigned counter;
        StartCounting(counter);

        std::ofstream s(filename);

        if (!s.good()) {
            ERROR("Unable to open file " << filename << " for writing");
        }

        s << "contentId" << ","
          << "rootCommitId" << ","
          << "projectId" << ","
          << "pathId" << ","
          << "modifications" << std::endl;

        for (auto &content : modifications) {
            unsigned content_id = content.first;
            for (auto &root_commit : content.second) {
                unsigned root_commit_id = root_commit.first;
                for (auto &project : root_commit.second) {
                    unsigned project_id = project.first;
                    for (auto &path : project.second) {
                        unsigned path_id = path.first;
                        unsigned modifications = path.second;

                        s << content_id << ","
                          << root_commit_id << ","
                          << project_id << ","
                          << path_id << ","
                          << modifications << std::endl;

                        // Count processed lines
                        Count(counter);
                    }
                }
            }
        }

        s.close();

        FinishCounting(counter, "clone modifications");
        FinishTask(task, timer);
    }

    void InspectFileClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        //Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_map<unsigned, std::unordered_set<unsigned>> commit_parents;
        LoadCommitParents(commit_parents);

        std::unordered_map<unsigned, std::unordered_set<unsigned>> project_commits;
        std::unordered_map<unsigned, std::unordered_set<unsigned>> commit_projects;
        std::unordered_map<unsigned, std::unordered_map<unsigned, unsigned>> commit_changes;
        LoadCommitChanges(project_commits,
                          commit_projects,
                          commit_changes);

        std::vector<FileCluster *> clusters;
        LoadFileCloneClusters(clusters);

        std::unordered_set<unsigned> projects_containing_clones;
        ExtractProjectsContainingClones(clusters,
                                        commit_projects,
                                        projects_containing_clones);

        std::unordered_map<unsigned, std::unordered_map<unsigned, Commit *>> project_commit_trees;
        PruneCommitTreesToProjects(commit_parents,
                                   project_commits,
                                   commit_projects,
                                   projects_containing_clones,
                                   project_commit_trees);

        std::unordered_map<unsigned,
                std::unordered_map<unsigned,
                        std::unordered_map<unsigned,
                                std::unordered_map<unsigned, unsigned>>>> modifications;
        AnalyzeCloneModifications(clusters,
                                  commit_projects,
                                  commit_changes,
                                  project_commit_trees,
                                  modifications);

        WriteOutModifications(modifications);
    }
    
} // namespace dejavu
