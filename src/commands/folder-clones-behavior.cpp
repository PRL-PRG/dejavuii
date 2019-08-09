#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <openssl/sha.h>
#include <fstream>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


namespace dejavu {

    namespace {

        class Clone {
        public:
            unsigned id;
            unsigned projectId;
            unsigned commitId;
            std::string path;

            Clone(unsigned id, unsigned projectId, unsigned commitId, std::string const & path):
                id(id),
                projectId(projectId),
                commitId(commitId),
                path(path),
                changingCommits{0},
                divergentCommits{0},
                syncCommits{0},
                syncDelay{0},
                fullySyncedTime{0}{
            }

            unsigned changingCommits;
            unsigned divergentCommits;
            unsigned syncCommits;
            uint64_t syncDelay;
            uint64_t fullySyncedTime;
            
        }; // Clone

        class Original {
        public:
            unsigned cloneId;
            unsigned projectId;
            unsigned commitId;
            std::string path;
            std::vector<Clone *> clones;

            Original(unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & path):
                cloneId(cloneId),
                projectId(projectId),
                commitId(commitId),
                path(path) {
            }

            void addClone(Clone * clone) {
                clones.push_back(clone);
            }
            
        }; // Original

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }

            uint64_t time2;
            bool tag;
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }

        };

        class State {
        public:
            State():
                active_(false) {
            }

            State(State const & from):
                active_(false) {
                mergeWith(from);
            }

            void mergeWith(State const & other, Commit * c = nullptr) {
                // this is ok, since if the numbers were different, the commit would change them anyways
                status.insert(other.status.begin(), other.status.end());
                active_ = active_ || other.active_;
            }

            bool active() const {
                return active_;
            }

            void setActive() {
                active_ = true;
            }

            /** First takes the */ 
            bool processCommit(Commit * c, std::string const & path, std::unordered_map<unsigned, std::string> const & paths) {
                bool result = false;
                for (unsigned pathId : c->deletions) {
                    std::string fp = getRelativePath(pathId, path, paths);
                    if (fp.empty())
                        continue;
                    status.erase(fp);
                    result = true;
                }
                for (auto ch : c->changes) {
                    std::string fp = getRelativePath(ch.first, path, paths);
                    if (fp.empty())
                        continue;
                    status[fp] = ch.second;
                    result = true;
                }
                return result;
            }

            /** Calculates the hash of the current state of the folder.
             */
            SHA1Hash hash() {
                std::stringstream s;
                for (auto i : status)
                    s << i.first << ":" << i.second << ",";
                SHA1Hash result;
                std::string x = s.str();
                SHA1((unsigned char *)x.c_str(), x.size(), (unsigned char *) & result.hash);
                return result;
            }

        private:

            std::string getRelativePath(unsigned pathId, std::string const & path, std::unordered_map<unsigned, std::string> const & paths) {
                std::string const & fp = paths.find(pathId)->second;
                if (helpers::startsWith(fp, path))
                    return fp.substr(path.size());
                else
                    return std::string{};
            }

            // path inside the folder -> contents id
            std::map<std::string, unsigned> status;

            bool active_;
        }; // STate

        
        
        class FolderCloneBehavior {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                    }};
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        paths_.insert(std::make_pair(id, path));
                    }};
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                    }};
                std::cerr << "Loading folder clone originals ..." << std::endl;
                FolderCloneOriginalsLoader([this](unsigned cloneId, SHA1Hash const &, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path, bool isOriginalClone){
                        originals_.insert(std::make_pair(cloneId, new Original(cloneId, projectId, commitId, path)));
                    });
                std::cerr << "Loading folder clone occurences ..." << std::endl;
                FolderCloneOccurencesLoader([this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & path, unsigned numFiles) {
                        originals_[cloneId]->addClone(new Clone(cloneId, projectId, commitId, path));
                    });
            }

            void analyzeClones() {
                std::cerr << "Analyzing clone behavior..." << std::endl;
                std::vector<std::thread> threads;
                auto i = originals_.begin();
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, this]() {
                        while (true) {
                            Original * o ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (i == originals_.end())
                                    return;
                                o = i->second;
                                ++i;
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (o == nullptr)
                                continue;
                            analyzeOriginal(o);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "Writing results..." << std::endl;
                std::ofstream f(DataDir.value() + "/folderCloneOccurencesBehavior.csv");
                f << "cloneId,projectId,commitId,path,changingCommits,divergentCommits,syncCommits,syncDelay,fullySyncedTime" << std::endl;
                for (auto i : originals_)
                    for (Clone * c : i.second->clones)
                        f << c->id << ","
                          << c->projectId << ","
                          << c->commitId << ","
                          << helpers::escapeQuotes(c->path) << ","
                          << c->changingCommits << ","
                          << c->divergentCommits << ","
                          << c->syncCommits << ","
                          << c->syncDelay << ","
                          << c->fullySyncedTime << std::endl;
            }

        private:

            /** Analyzes single original and its clones.

                First we must calculate the state of the folder in the original at various times so that we can later compare this. 
             */
            void analyzeOriginal(Original * o) {
                // calculate a map of different states of the original and the times
                std::unordered_map<SHA1Hash, uint64_t> originalContents;
                fillOriginalContents(o, originalContents);
                std::vector<std::pair<uint64_t, SHA1Hash>> sortedContents;
                {
                    std::map<uint64_t, SHA1Hash> sorted;
                    for (auto i : originalContents)
                        sorted.insert(std::make_pair(i.second, i.first));
                    for (auto i : sorted)
                        sortedContents.push_back(i);
                }
                for (Clone * c : o->clones)
                    analyzeClone(c, originalContents, sortedContents);
            }

            void fillOriginalContents(Original * o,  std::unordered_map<SHA1Hash, uint64_t> & contents) {
                Project * p = projects_[o->projectId];
                CommitForwardIterator<Project, Commit, State> i(p, [&, this](Commit * c, State & state){
                        if (state.processCommit(c, o->path, paths_))
                            contents.insert(std::make_pair(state.hash(), c->time));
                        return true;
                    });
                i.process();
            }


            /** Analyzes the clone and calculates the following metrics:

                Time spent 100% synchronized - for each change in clone we check if parent has the same state at the time and if it does we add the time during which they are identical to the overall time.

                Divergent commits - when a commit changes the clone, determine if the change was *ever* observed in the original. If not, the commit is divergent.

                Synchronization time - for each non-divergent commit calculate the time it took to propagate the commit from original to clone. 

             */
            void analyzeClone(Clone * clone, std::unordered_map<SHA1Hash, uint64_t> const & originalContents, std::vector<std::pair<uint64_t, SHA1Hash>> const & sortedContents) {
                Project * p = projects_[clone->projectId];
                std::map<uint64_t, SHA1Hash> cloneSorted;
                CommitForwardIterator<Project, Commit, State> i(p, [&, this](Commit * c, State & state){
                        if (! state.active() && clone->commitId == c->id)
                            state.setActive();
                        if (state.active()) {
                            if (state.processCommit(c, clone->path, paths_)) {
                                SHA1Hash hash = state.hash();
                                cloneSorted.insert(std::make_pair(c->time, hash));
                                if (clone->commitId != c->id) {
                                    ++clone->changingCommits;
                                    auto i = originalContents.find(hash);
                                    if (i == originalContents.end()) {
                                        ++clone->divergentCommits;
                                    } else {
                                        uint64_t originalTime = i->second;
                                        if (originalTime <= c->time) {
                                            ++clone->syncCommits;
                                            clone->syncDelay += (c->time - originalTime);
                                        }
                                    }
                                }
                            }
                        }
                        return true;
                    });
                i.process();
                // now we must calculate the fully synced time
                size_t o = 0;
                uint64_t tmax = std::max(cloneSorted.rbegin()->first, sortedContents.back().first);
                for (auto c = cloneSorted.begin(), ce = cloneSorted.end(); c != ce;) {
                    while (o != sortedContents.size() - 1) {
                        if (sortedContents[o].first == c->first)
                            break;
                        if (sortedContents[o].first > c->first) {
                            --o;
                            break;
                        }
                    }
                    if (c->second == sortedContents[o].second) {
                        uint64_t start = c->first;
                        uint64_t end = tmax;
                        if (++c != ce && c->first < end)
                                end = c->first;
                        if (o != sortedContents.size() - 1 && sortedContents[o + 1].first < end)
                            end = sortedContents[o + 1].first;
                        clone->fullySyncedTime += end - start;
                    } else {
                        ++c;
                    }
                }
            }

            size_t getTimeIndex(uint64_t time, std::vector<std::pair<uint64_t, SHA1Hash>> const & sorted) {
                // let's do really stupid linear search for now
                for (size_t i = 0; i < sorted.size(); ++i) {
                    if (sorted[i].first == time)
                        return i;
                    if (sorted[i].first > time)
                        return i - 1;
                }
                return sorted.size() - 1;
            }
            

            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, std::string> paths_;
            std::unordered_map<unsigned, Original *> originals_;

            std::mutex mCerr_;
        }; // FolderCloneBehavior

    } // anonymous namespace




    void FolderClonesBehavior(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        FolderCloneBehavior fcb;
        fcb.loadData();
        fcb.analyzeClones();


        
    }


    
} // namespace dejavu
