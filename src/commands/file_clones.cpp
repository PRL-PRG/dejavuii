#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {
        class Commit;

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                    BaseProject<Project, Commit>(id, createdAt) {
            }
            static void LoadProjects(std::unordered_map<unsigned, Project *> &projects) {
                clock_t timer;
                std::string task = "extracting project information (all projects)";
                helpers::StartTask(task, timer);

                std::unordered_set<unsigned> insertedProjects;
                new ProjectLoader([&](unsigned id, std::string const & user,
                                      std::string const & repo, uint64_t createdAt){
                    assert(projects.find(id) == projects.end());
                    projects[id] = new Project(id, createdAt);
                });

                std::cerr << "Loaded " << projects.size() << " projects" << std::endl;

                helpers::FinishTask(task, timer);
            }
        };

        class Commit : public BaseCommit<Commit>{
        public:
            Commit(unsigned int id, uint64_t time) : BaseCommit(id, time) {}

            static void LoadCommits(std::unordered_map<unsigned, Commit *> &commits) {
                std::string task = "loading commits";
                clock_t timer;
                helpers::StartTask(task, timer);

                new CommitLoader([&](unsigned commitId, uint64_t authorTime,
                                     uint64_t committerTime) {
                    assert(commits.find(commitId) == commits.end());
                    Commit *commit = new Commit(commitId, authorTime);
                    commits[commitId] = commit;
                });

                helpers::FinishCounting(commits.size(), "commits");
                helpers::FinishTask(task, timer);
            }

            static void LoadCommitParents(std::unordered_map<unsigned, Commit *> &commits) {
                std::string task = "loading commit parents";
                clock_t timer;
                unsigned relations;
                helpers::StartTask(task, timer);

                new CommitParentsLoader(
                        [&](unsigned childId, unsigned parentId) {
                            assert(commits.find(childId) !=
                                   commits.end());
                            assert(commits.find(parentId) !=
                                   commits.end());
                            Commit *child = commits[childId];
                            Commit *parent = commits[parentId];
                            child->addParent(parent);
                            //parent->addChild(child); already done in addParent
                            ++relations;
                        });

                helpers::FinishCounting(relations, "parent-child relations");
                helpers::FinishTask(task, timer);
            }

            static void LoadCommitFileChanges(std::unordered_map<unsigned, Commit *> &commits,
                                              std::unordered_map<unsigned, Project *> &projects) {
                std::string task = "loading file changes (into commits)";
                clock_t timer;
                unsigned changes;
                helpers::StartTask(task, timer);

                new FileChangeLoader([&](unsigned projectId, unsigned commitId,
                                         unsigned pathId, unsigned contentsId){
                    assert(commits.find(commitId) != commits.end());
                    Commit *commit = commits[commitId];
                    commit->addChange(pathId,contentsId);

                    assert(projects.find(projectId) != projects.end());
                    projects[projectId]->addCommit(commit);

                    ++changes;
                });

                helpers::FinishCounting(changes, "file changes");
                helpers::FinishTask(task, timer);
            }
        };

        class Modification {
        public:
            Modification(unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id, unsigned timestamp) :
                    project_id(project_id), commit_id(commit_id), path_id(path_id), contents_id(contents_id), timestamp(timestamp) {};


            const unsigned project_id;
            const unsigned commit_id;
            const unsigned path_id;
            const unsigned contents_id;
            const u_int64_t timestamp = 0L;

        protected:
            bool original = false;

            friend class ModificationCluster;
        };

        class ModificationCluster {
        public:
            ModificationCluster(std::vector<Modification *> modifications) : modifications(modifications) {
                // Elect first modification as provisionally oldest.
                Modification *oldest = modifications[0];

                // Select oldest modification.
                for (int i = 1, size = modifications.size(); i < size; i++) {
                    if (modifications[i]->timestamp < oldest->timestamp) {
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

            static void CountRepeats(std::unordered_map<unsigned, unsigned> &counters) {
                std::string task = "counting repeating file contents";
                unsigned contents;
                clock_t timer;

                helpers::StartTask(task, timer);

                FileChangeLoader([&](unsigned project_id, unsigned commit_id, unsigned path_id, unsigned contents_id) mutable {
                        counters[contents_id]++;
                        ++contents;
                });

                helpers::FinishCounting(contents, "file contents");
                helpers::FinishTask(task, timer);
            }

            static void CountPluralContentClusters(std::unordered_map<unsigned, unsigned> &counters) {
                std::string task = "counting plural content clusters: file content clusters with more than one member ";
                size_t contents = 0;
                size_t pluralities = 0;
                clock_t timer;

                helpers::StartTask(task, timer);

                for (auto it : counters) {
                    if (it.second > 1 && it.first != 0 /* exclude empty files */) {
                        ++pluralities;
                    }
                    helpers::Count(contents);
                }

                helpers::FinishCounting(pluralities, "plural clusters");
                helpers::FinishCounting(contents, "all content clusters");
                helpers::FinishTask(task, timer);
            }

            static void LoadFileChanges(std::unordered_map<unsigned, Commit *> const &commits,
                                        std::unordered_map<unsigned, unsigned> &counters,
                                        std::unordered_map<unsigned, std::vector<Modification*>> & clusters) {
                std::string task = "loading file changes (into plural content clusters)";
                clock_t timer;
                helpers::StartTask(task, timer);

                unsigned skipped=0, pluralities=0;

                FileChangeLoader([&](unsigned project_id,
                                     unsigned commit_id,
                                     unsigned path_id,
                                     unsigned contents_id) mutable {
                    if (counters.at(contents_id) < 2 && contents_id != 0  /* exclude empty files */) {
                        skipped++;
                        return;
                    }
                    clusters[contents_id].push_back(new Modification(project_id,
                                                                     commit_id,
                                                                     path_id,
                                                                     contents_id,
                                                                     commits.at(commit_id)->time));
                    pluralities++;
                });

                std::cerr << "processed " << pluralities << " plural clusters" << std::endl;
                std::cerr << "skipped " << skipped << " singular clusters" << std::endl;

                helpers::FinishTask(task, timer);
            }

            static void MarkOriginalClonesInClusters(std::unordered_map<unsigned, std::vector<Modification*>> const &vectorClusters,
                                                     std::unordered_map<unsigned, ModificationCluster*> &clusters) {
                clock_t timer;
                std::string task = "marking original clones in clusters";
                helpers::StartTask(task, timer);

                size_t counter;
                helpers::StartCounting(counter);

                for (auto &it : vectorClusters) {
                    // Mark oldest modification in cluster;
                    ModificationCluster *cluster = new ModificationCluster(it.second);
                    clusters[it.first] = cluster;

                    // Count processed lines
                    helpers::Count(counter);
                }

                helpers::FinishCounting(counter, "clusters");
                helpers::FinishTask(task, timer);
            }

            static void SaveOriginalClones(std::unordered_map<unsigned, ModificationCluster *> const &clusters) {
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

                size_t counter = 0;
                helpers::StartCounting(counter);

                for (auto & it : clusters) {
                    ModificationCluster *cluster = it.second;
                    Modification *original = cluster->original;
                    s << original->contents_id << ","
                      << 1 << ","
                      << original->project_id << ","
                      << original->commit_id << ","
                      << original->path_id << ","
                      << std::endl;

                    helpers::Count(counter);
                }

                s.close();

                helpers::FinishCounting(counter, "clone clusters");
                helpers::FinishTask(task, timer);
            }

            static void SaveClusters(std::unordered_map<unsigned, ModificationCluster *> const &clusters) {
                const std::string filename = DataDir.value() + "/fileClusters.csv";

                clock_t timer;
                std::string task = "Saving clusters " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);

                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "contentId" << ","
                  << "clusterSize" << ","
                  << "originalCommitId" << std::endl;

                size_t counter;
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

            static void SaveClusterCommits(std::unordered_map<unsigned, ModificationCluster *> const &clusters) {
                const std::string filename = DataDir.value() + "/fileClustersWithCommits.csv";

                clock_t timer;
                std::string task = "Saving clusters (with commit information) to " + filename;
                helpers::StartTask(task, timer);

                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                s << "contentId" << ","
                  << "clusterSize" << ","
                  << "originalCommitId" << ","
                  << "commitList" << std::endl;

                size_t counter;
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

            std::vector<Modification *> modifications;
            Modification * original;

            //static std::unordered_map<unsigned, ModificationCluster*> clusters;
        };

        //std::unordered_map<unsigned, ModificationCluster*> ModificationCluster::clusters;
    } //anonymoose namespace

    class ClusterInfo {
    public:
        unsigned notFromEmpty;
        unsigned contentsId;
        unsigned changes;
        unsigned deletions;

        ClusterInfo(bool notFromEmpty, unsigned contentsId):
            notFromEmpty(notFromEmpty),
            contentsId(contentsId),
            changes(0),
            deletions(0) {
        }

        friend std::ostream & operator << (std::ostream & s, ClusterInfo const & ci) {
            s << ci.contentsId << "," << ci.notFromEmpty << "," << ci.changes << "," << ci.deletions;
            return s;
        }
    };


    class ProjectState {
    public:

        ProjectState() {
        }

        ProjectState(ProjectState const & other):
            trackedFiles_(other.trackedFiles_) {
        }

        void mergeWith(ProjectState const & other, Commit *c) {
            for (auto tf : other.trackedFiles_) {
                auto & myTracked = trackedFiles_[tf.first];
                if (tf.second.contents == c->changes[tf.first]) {
                    myTracked.clusterInfos = tf.second.clusterInfos;
                    myTracked.selected = true;
                } else if (!myTracked.selected) {
                    for (auto ci : tf.second.clusterInfos)
                        myTracked.clusterInfos.insert(ci);
                }
            }
        }

        void recordCommit(Commit * c, std::unordered_set<unsigned> const & contentsToBeTracked, std::vector<ClusterInfo *> & clusterInfos) {
            for (unsigned path : c->deletions) {
                for (ClusterInfo * ci : trackedFiles_[path].clusterInfos)
                    ++ci->deletions;
                trackedFiles_.erase(path);
            }

            for (auto change : c->changes) {
                auto i = trackedFiles_.find(change.first);
                bool notFromEmpty = true;
                if (i == trackedFiles_.end()) {
                    notFromEmpty = false;
                    i = trackedFiles_.insert(std::make_pair(change.first, FileInfo())).first;
                }
                FileInfo & fi = i->second;
                fi.contents = change.second;
                fi.selected = false;
                if (fi.clusterInfos.empty()) {
                    auto x = contentsToBeTracked.find(change.second);
                    if (x != contentsToBeTracked.end()) {
                        ClusterInfo * ci = new ClusterInfo(notFromEmpty, change.second);
                        fi.clusterInfos.insert(ci);
                        clusterInfos.push_back(ci);
                    }
                } else {
                    for (auto ch : fi.clusterInfos)
                        ++ch->changes;
                }
            }
        }

        struct FileInfo {
            unsigned contents;
            bool selected;
            std::unordered_set<ClusterInfo *> clusterInfos;

            FileInfo():
                contents(FILE_DELETED),
                selected(false) {
            }
        };

        std::unordered_map<unsigned, FileInfo> trackedFiles_;
    };

    class ChangesDetector {
    public:
        ChangesDetector(std::unordered_set<unsigned> const &clusterIds):
                contentsToBeTracked_(clusterIds) {
        }

        static void ExtractClusterIds(std::unordered_set<unsigned> &clusterIds) {
            clock_t timer;
            std::string task = "loading file clone clusters (to get IDs)";
            helpers::StartTask(task, timer);

            FileClonesLoader([&clusterIds](unsigned content_id,
                                          unsigned cluster_size,
                                          unsigned original_commit_id) {
                clusterIds.insert(content_id);
            });

            helpers::FinishTask(task, timer);
        }

        static void ExtractProjectsContainingClones(std::unordered_set<unsigned> const &clusterIds,
                                                    std::unordered_set<unsigned> &projectsContainingClones) {
            clock_t timer;
            std::string task = "extracting projects containing clones";
            helpers::StartTask(task, timer);

            FileChangeLoader([&](unsigned project_id,
                                 unsigned commit_id,
                                 unsigned path_id,
                                 unsigned contents_id) mutable {

                if (clusterIds.find(contents_id) != clusterIds.end()) {
                    projectsContainingClones.insert(project_id);
                }
            });

            std::cerr << "Found " << projectsContainingClones.size()
                      << " interesting projects";

            helpers::FinishTask(task, timer);
        }

        static void FilterProjects(std::unordered_map<unsigned, Project *> const &allProjects,
                                   std::unordered_set<unsigned> const &interestingProjectIds,
                                   std::vector<Project *> &interestingProjects) {
            clock_t timer;
            std::string task = "filtering out interesting projects";
            size_t projectCount = 0;
            helpers::StartTask(task, timer);

            for (auto id : interestingProjectIds) {
                helpers::Count(projectCount);
                interestingProjects.push_back(allProjects.at(id));
            }

            helpers::FinishCounting(projectCount, "projects");
            helpers::FinishTask(task, timer);
        }

        void analyze(std::vector<Project *> const & projects) {
            std::string path = DataDir.value() + "/fileCloneChanges.csv";
            clock_t timer;
            std::string task = "analyzing commit graphs (writing results to " + path + ")";
            helpers::StartTask(task, timer);

            std::cerr << "TODO: " << projects.size() << " projects" << std::endl;

            fOut_.open(path);
            fOut_ << "#projectId,clusterId,notFromEmpty,changes,deletions" << std::endl;
            
            std::vector<std::thread> threads;
            size_t completed = 0;

            for (unsigned stride = 0; stride < NumThreads.value(); ++stride) {

                threads.push_back(std::thread([stride, &completed, this, &projects]() {
                    while (true) {
                        Project *p;
                        {
                            std::lock_guard<std::mutex> g(mCerr_);
                            if (completed == projects.size())
                                return;
                            p = projects.at(completed);
                            helpers::Count(completed);
                        }
                        if (p == nullptr)
                            continue;
                        detectChangesInProject(p);
                    }
                }));
            }

            for (auto & i : threads)
                i.join();

            helpers::FinishCounting(completed, "projects");
            helpers::FinishTask(task, timer);
        }

    private:
        void detectChangesInProject(Project *p) {
            std::vector<ClusterInfo *> clusterInfos;

            CommitForwardIterator<Project, Commit, ProjectState> cfi(p, [this, &clusterInfos](Commit *c, ProjectState &state) {
                    state.recordCommit(c, contentsToBeTracked_, clusterInfos);
                    return true;
                });

            cfi.process();

            // clusterInfos contain information about all clusters found in the project
            {
                std::lock_guard<std::mutex> g(mOut_);
                for (ClusterInfo * ci : clusterInfos)
                    fOut_ << p->id << "," << *ci << std::endl;
            }

            for (ClusterInfo * ci : clusterInfos)
                delete ci;
        }


        //std::vector<Project *> projects_;
        //std::vector<Commit *> commits_;
        
        std::unordered_set<unsigned> contentsToBeTracked_;

        std::ofstream fOut_;
        std::mutex mOut_;
        std::mutex mCerr_;
    };

    void DetectFileClones2(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_map<unsigned, Project *> projects;
        Project::LoadProjects(projects);

        std::unordered_map<unsigned, Commit *> commits;
        Commit::LoadCommits(commits);
        Commit::LoadCommitParents(commits);
        Commit::LoadCommitFileChanges(commits, projects);

        // The big stuff.
        std::unordered_map<unsigned, unsigned> counters;
        ModificationCluster::CountRepeats(counters);
        ModificationCluster::CountPluralContentClusters(counters);

        std::unordered_map<unsigned, std::vector<Modification*>> vectorClusters;
        ModificationCluster::LoadFileChanges(commits, counters, vectorClusters);

        std::unordered_map<unsigned, ModificationCluster*> clusters;
        ModificationCluster::MarkOriginalClonesInClusters(vectorClusters, clusters);

        ModificationCluster::SaveOriginalClones(clusters);
        ModificationCluster::SaveClusters(clusters);
        //ModificationCluster::SaveClusterCommits(clusters);

        // Can start from here.
        std::unordered_set<unsigned> clusterIds;//(clusters.begin(), clusters.end());
        ChangesDetector::ExtractClusterIds(clusterIds);

        std::unordered_set<unsigned> interestingProjectIds;
        ChangesDetector::ExtractProjectsContainingClones(clusterIds, interestingProjectIds);

        std::vector<Project *> interestingProjects;
        ChangesDetector::FilterProjects(projects, interestingProjectIds, interestingProjects);

        ChangesDetector cd(clusterIds);
        cd.analyze(interestingProjects);
    }
    
} // namespace dejavu
