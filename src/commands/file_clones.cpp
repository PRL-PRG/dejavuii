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
        class Commit : public BaseCommit<Commit>{
        public:
            Commit(unsigned int id, uint64_t time) : BaseCommit(id, time) {}

            static Commit *Get(unsigned id) {
                assert(Commit::commits_.find(id) != commits_.end());
                return Commit::commits_[id];
            }

            static void LoadCommits() {
                new CommitLoader([&](unsigned commitId, uint64_t authorTime, uint64_t committerTime){
                    assert(Commit::commits_.find(commitId) == commits_.end());

                    Commit *commit = new Commit(commitId, authorTime);
                    Commit::commits_[commitId] = commit;
                });

                new CommitParentsLoader([&](unsigned childId, unsigned parentId){
                    assert(Commit::commits_.find(childId) != commits_.end());
                    assert(Commit::commits_.find(parentId) != commits_.end());
                    Commit *child = Commit::Get(childId);
                    Commit *parent = Commit::Get(parentId);
                    child->addParent(parent);
                    //parent->addChild(child); already done in addParent
                });

                new FileChangeLoader([&](unsigned projectId, unsigned commitId,
                                         unsigned pathId, unsigned contentsId){
                    assert(Commit::commits_.find(commitId) != commits_.end());
                    Commit *commit = Commit::Get(commitId);
                    commit->addChange(pathId,contentsId);
                });
            }

        private:
            static std::unordered_map<unsigned, Commit *> commits_;
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                    BaseProject<Project, Commit>(id, createdAt) {
            }
        };

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
                oldest->timestamp = Commit::Get(oldest->commit_id)->time;

                // Select oldest modification.
                for (int i = 1, size = modifications.size(); i < size; i++) {
                    if (modifications[i]->timestamp < oldest->timestamp) {
                        modifications[i]->timestamp = Commit::Get(modifications[i]->commit_id)->time;
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

                std::cerr << "CONTING CONTENT KLUSTERS" << std::endl;
                int pluralities = 0;
                for (auto it : counters) {
                    if (it.second > 1) {
                        pluralities++;
                    }
                }
                std::cerr << "DER " << pluralities << "CKONTENT CKLUSTERZ" << std::endl;

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
                int counter = 0;
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
                std::cerr << " : " <<(counter / 1000) << "k" << std::endl;
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
                std::cerr << " : " <<(counter / 1000) << "k" << std::endl;
                std::cerr << "DONE WRITINK OUT KLUSTER INFORMESHON" << std::endl;
            }

        protected:
            std::vector<Modification *> modifications;
            Modification * original;

            static std::unordered_map<unsigned, ModificationCluster*> clusters;
        };

        std::unordered_map<unsigned, ModificationCluster*> ModificationCluster::clusters;
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

        ProjectState(ProjectState const & other) {
            mergeWith(other, nullptr); // FIXME
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
        void analyze() {
            fOut_.open(DataDir.value() + "/fileCloneChanges.csv");
            fOut_ << "#projectId,clusterId,notFromEmpty,changes,deletions" << std::endl;
            
            std::vector<std::thread> threads;
            size_t completed = 0;

            for (unsigned stride = 0; stride < NumThreads.value(); ++stride) {
                std::thread t = std::thread([stride, &completed, this]() {
                    while (true) {
                        Project *p;
                        {
                            std::lock_guard<std::mutex> g(mCerr_);
                            if (completed == projects_.size())
                                return;
                            p = projects_[completed];
                            ++completed;
                            if (completed % 1000 == 0)
                                std::cerr << " : " << completed << "    \r"
                                          << std::flush;
                        }
                        if (p == nullptr)
                            continue;
                        detectChangesInProject(p);
                    }
                });
                threads.push_back(t);
            }

            for (auto & i : threads)
                i.join();
        }
        
    private:
        void detectChangesInProject(Project *p) {
            std::vector<ClusterInfo *> clusterInfos;
            CommitForwardIterator<Project, Commit, ProjectState> cfi(p, [this](Commit *c, ProjectState & state) {
                    state.recordCommit(c, contentsToBeTracked_, clusterInfos);
                    return true;
                });

            // clusterInfos contain information about all clusters found in the project
            {
                std::lock_guard<std::mutex> g(mOut_);
                for (ClusterInfo * ci : clusterInfos)
                    fOut_ << p->id << "," << *ci << std::endl;
            }

            for (ClusterInfo * ci : clusterInfos)
                delete ci;
        }


        std::vector<Project *> projects_;
        //std::vector<Commit *> commits_;
        
        std::unordered_set<unsigned> contentsToBeTracked_;

        std::ofstream fOut_;
        std::mutex mOut_;
        std::mutex mCerr_;
    };

    void DetectFileClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Commit::LoadCommits();
        ModificationCluster::LoadClusters();
        ModificationCluster::SaveClusters();
    }
    
} // namespace dejavu
