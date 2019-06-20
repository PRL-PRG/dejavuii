#include <unordered_map>
#include <map>
#include <vector>
#include <thread>
#include <mutex>
#include <limits>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


namespace dejavu {

    namespace {

        class Clone;
        class Project;
        class TimeAggregator;

        /** Information about aggregated numbers of different files at a time.

            The times for which TimeInfo is provided are discrete and reflect the situation *after* all commits happening at the particular time. 
         */
        class Stats {
        public:

            long projects;
            
            /** Total number of files at the given time.
             */
            long files;

            /** Number of files that are in the node_modules folder.
             */
            long npmFiles;

            /** Number of files that are clones, i.e. files whose contents we have already seen elsewhere.
             */
            long clones;

            /** Number of files that we have seen elsewhere that reside in node_modules folder.
             */
            long npmClones;

            /** Number of files which belong to a folder clone and have not been changed.
             */
            long folderClones;

            /** Number of unchanged files in node_modules directories belonging to a folder clone.
             */
            long npmFolderClones;

            /** Number of files belonging to a folder clone that have been changed since cloned.
             */
            long changedFolderClones;

            /** Number of files belonging to a node_modules folder which are part of a folder clone, but have been changed since they were cloned.
             */
            long npmChangedFolderClones;

            Stats():
                projects(0),
                files(0), npmFiles(0), clones(0), npmClones(0), folderClones(0), npmFolderClones(), changedFolderClones(0), npmChangedFolderClones(0) {
            }

            Stats operator + (Stats const & other) const {
                Stats result;
                result.projects = projects + other.projects;
                result.files = files + other.files;
                result.npmFiles = npmFiles + other.npmFiles;
                result.clones = clones + other.clones;
                result.npmClones = npmClones + other.npmClones;
                result.folderClones = folderClones + other.folderClones;
                result.npmFolderClones = npmFolderClones + other.npmFolderClones;
                result.changedFolderClones = changedFolderClones + other.changedFolderClones;
                result.npmChangedFolderClones = npmChangedFolderClones + other.npmChangedFolderClones;
                return result;
            }

            Stats operator - (Stats const & other) const {
                Stats result;
                result.projects = projects - other.projects;
                result.files = files - other.files;
                result.npmFiles = npmFiles - other.npmFiles;
                result.clones = clones - other.clones;
                result.npmClones = npmClones - other.npmClones;
                result.folderClones = folderClones - other.folderClones;
                result.npmFolderClones = npmFolderClones - other.npmFolderClones;
                result.changedFolderClones = changedFolderClones - other.changedFolderClones;
                result.npmChangedFolderClones = npmChangedFolderClones - other.npmChangedFolderClones;
                return result;
            }

            Stats & operator += (Stats const & other) {
                projects += other.projects;
                files += other.files;
                npmFiles += other.npmFiles;
                clones += other.clones;
                npmClones += other.npmClones;
                folderClones += other.folderClones;
                npmFolderClones += other.npmFolderClones;
                changedFolderClones += other.changedFolderClones;
                npmChangedFolderClones += other.npmChangedFolderClones;
                return *this;
            }
            
        }; // Stats

        class CloneOriginal;

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }

            /** path -> clone original
             */
            std::unordered_map<std::string, CloneOriginal *> addedClones;
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }

            Stats stats;
        };

        class CloneOriginal {
        public:
            Project * project;
            Commit * commit;
            std::string root;
            bool isClone;

            CloneOriginal(Project * p, Commit * c, std::string const & path, bool isClone):
                project(p),
                commit(c),
                root(path),
                isClone(isClone) {
            }

            bool isOriginal(Project * p, Commit * c, std::string const & path) {
                if (isClone)
                    return false;
                return commit == c && project == p && root == path;
            }
        }; 


        /** Holds clone-specific information about a path.

            Marks whether the patrh holds a file clone, folder clone, or a changed folder clone. 
         */
        class PathInfo {
        public:
            bool clone;
            bool folderClone;
            bool changedFolderClone;

            PathInfo():
                clone(false),
                folderClone(false),
                changedFolderClone(false) {
            }

            void setAsFolderClone() {
                //assert(clone);
                folderClone = true;
                changedFolderClone =false;
            }
        };

        class FileOriginalInfo {
        public:
            Project * project;
            Commit * commit;
            unsigned pathId;

            FileOriginalInfo():
                project(nullptr),
                commit(nullptr),
                pathId(0) {
            }

            void updateWith(Project * p, Commit * c, unsigned path, std::vector<std::pair<std::string, bool>> const & paths) {
                if (project == nullptr || IsBetterOriginal(project, commit, paths[pathId].first, p, c, paths[path].first)) {
                    project = p;
                    commit = c;
                    pathId = path;
                }
            }
        };

        /** Commit snapshots aggregates information about all files alive at any given commit.

            This means that for a path there can only be *one* version, as opposed to the TimeSnapshot defined later, which allows multiple versions of single path to exist. 
         */
        class CommitSnapshot {
        public:

            /** In addition to path info we also need the contents of the file so that merges can be performed correctly.
             */
            class CommitPathInfo : public PathInfo {
            public:
                unsigned contentsId;

                CommitPathInfo():
                    contentsId(0) {
                }
            };
            
            CommitSnapshot() {
            }

            CommitSnapshot(CommitSnapshot const & from):
                files_(from.files_) {
            }

            /** Merges with other state.

                Copies all files from the other state to the current state. However, care must be taken when the same path already exists in the state - in this case the merge commit may simply decide which version of the file will be used and therefore if the contents of the path of the other state matches the contents to which the commit will set the file, we simply copy the state verbatim. Otherwise we leave it be and deal with it in the update state. 
             */
            void mergeWith(CommitSnapshot const & from, Commit * c) {
                for (auto i : from.files_) {
                    auto j = files_.find(i.first);
                    // if the file is not found in current state, simply add its info 
                    if (j == files_.end()) {
                        files_.insert(i);
                     // otherwise, if the incomming contents is identical to the contents set by the commit, replace the pathInfo, ignore in other cases 
                    } else {
                        auto ci = c->changes.find(i.first);
                        if (ci != c->changes.end()) {
                            if (j->second.contentsId == ci->second)
                                j->second = i.second;
                        }
                    }
                }
            }

            void updateWith(Project * p, Commit * c, std::unordered_map<unsigned,  FileOriginalInfo> & fileOriginals, std::vector<std::pair<std::string, bool>> const & paths) {
                // firt delete what must be deleted
                for (unsigned pathId : c->deletions)
                    files_.erase(pathId);
                // the update the state based on changes in current commit
                for (auto i : c->changes) {
                    CommitPathInfo & pi = files_[i.first];
                    // if the contents id of the stored path info matches the one of the commit change, then it is not a change, but was a merge selection dealth with in merge
                    if (pi.contentsId == i.second)
                        continue;
                    // set the proper contents id, update the clone state
                    bool npm = paths[i.first].second;
                    pi.contentsId = i.second;
                    ++p->stats.files;
                    if (npm)
                        ++p->stats.npmFiles;
                    bool x = isClone(i.second, i.first, p, c, fileOriginals);
                    if (x) {
                        ++p->stats.clones;
                        if (npm)
                            ++p->stats.npmClones;
                        pi.clone = true;
                        if (pi.folderClone) {
                            pi.changedFolderClone = true;
                            ++p->stats.changedFolderClones;
                            if (npm)
                                ++p->stats.npmChangedFolderClones;
                        }
                    }                         
                }
                // finally, if the commit introduces any folder clones, we must mark them as such
                for (auto clone : c->addedClones) {
                    std::string const & root = clone.first;
                    if (p->id == 43 && c->id == 7019)
                        std::cout << "Testing root " << root << std::endl;
                    // We don't have to maintain project with folder clones, because if there is a clone in commit, then the clone happens in *all* projects containing the clone. The only exception is if the original is itself a clone and happens to be in the oldest of projects containing the clone, which is what we check here (or if the original is same project, same commit, different path), both are degenerate cases
                    if (! IgnoreFolderOriginals.value() && clone.second->isOriginal(p, c, root))
                        continue;
                    for (auto i : c->changes) {
                        if (paths[i.first].first.find(root) == 0) {
                            /*
                            if (! IgnoreFolderOriginals.value()) {
                                if (!files_[i.first].clone) {
                                    std::cerr << "Expected clone: " << p->id << "," << c->id << "," << i.first << "," << i.second << std::endl;
                                    std::cerr << "Original: " << fileOriginals[i.second].project->id << ","  << fileOriginals[i.second].commit->id << "," << fileOriginals[i.second].pathId << std::endl;
                                    std::cerr << "root: " << root << std::endl;
                                }
                            }
                            */
                            files_[i.first].setAsFolderClone();
                            ++p->stats.folderClones;
                            if (paths[i.first].second)
                                ++p->stats.npmFolderClones;
                        } else {
                            if (p->id == 43 && c->id == 7019)
                                std::cout << paths[i.first].first << " -- NOT A CLONE" << std::endl;
                        }
                    }
                }
            }

        private:
            friend class TimeSnapshot;

            bool isClone(unsigned contentsId, unsigned pathId, Project * p, Commit * c, std::unordered_map<unsigned, FileOriginalInfo> const & fileOriginals) {
                auto i = fileOriginals.find(contentsId);
                assert(i != fileOriginals.end());
                // if the path, commit or project does not match, we have clone, 
                if (pathId != i->second.pathId)
                    return true;
                if (c != i->second.commit)
                    return true;
                if (p != i->second.project)
                    return true;
                return false; // it's original, they all match
            }

            
            std::unordered_map<unsigned, CommitPathInfo> files_;
        };

        /** Time snapshot is similar to clone snapshot, but may contain different versions of same path at the same time.

            This is done by having the files map index based on both contents and path ids.
         */
        class TimeSnapshot {
        public:

            void updateWith(CommitSnapshot const & cs) {
                for (auto i : cs.files_) {
                    size_t index = (static_cast<size_t>(i.second.contentsId) << 32) + i.first;
                    auto j = files_.find(index);
                    if (j == files_.end())
                        files_.insert(std::make_pair(index, static_cast<PathInfo>(i.second)));
                }
            }

            Stats getStats(std::vector<std::pair<std::string, bool>> const & paths) {
                Stats result;
                for (auto i : files_) {
                    unsigned pathId = i.first & 0xffffffff; // mask only the path id
                    assert(pathId < paths.size());
                    bool isNPM = paths[pathId].second;
                    ++result.files;
                    if (i.second.clone)
                        ++result.clones;
                    if (i.second.folderClone)
                        ++result.folderClones;
                    if (i.second.changedFolderClone)
                        ++result.changedFolderClones;
                    if (isNPM) {
                        ++result.npmFiles;
                        if (i.second.clone)
                            ++result.npmClones;
                        if (i.second.folderClone)
                            ++result.npmFolderClones;
                        if (i.second.changedFolderClone)
                            ++result.npmChangedFolderClones;
                    }
                    assert(! i.second.changedFolderClone || i.second.folderClone);
                }
                return result;
            }

        private:
            std::unordered_map<size_t, PathInfo> files_;
            
        };

        
        /** Aggregates the number of clones over time.

            How to aggreegate over the projects w/o the need to go through all commit times? 
         */
        class TimeAggregator {
        public:

            void initialize() {
                std::cerr << "Loading projects..." << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        if (projects_.size() <= id)
                            projects_.resize(id + 1);
                        projects_[id] = new Project(id, createdAt);
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                std::cerr << "Loading commits..." << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        if (commits_.size() <= id)
                            commits_.resize(id + 1);
                        commits_[id] = new Commit(id, authorTime);
                    }};
                std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;
                std::cerr << "Loading commit parents... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        assert(c != nullptr);
                        Commit * p = commits_[parentId];
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[this](unsigned id, std::string const & path){
                        if (paths_.size() <= id)
                            paths_.resize(id + 1);
                        //std::string p = std::string("/" + path);
                        paths_[id] = std::make_pair(path, IsNPMPath(path));
                    }};
                std::cerr << "    " << paths_.size() << " paths loaded" << std::endl;
                std::string occurencesPath = DataDir.value() + "/folderCloneOccurences.csv";
                if (! IgnoreFolderOriginals.value()) {
                    std::cerr << "Loading clone originals..." << std::endl;
                    FolderCloneOriginalsLoader{[this](unsigned cloneId, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path, bool isOriginalClone){
                            Commit * c = commits_[commitId];
                            Project * p = projects_[projectId];
                            if (cloneOriginals_.size() <= cloneId)
                                cloneOriginals_.resize(cloneId + 1);
                            cloneOriginals_[cloneId] = new CloneOriginal(p, c, path, isOriginalClone);
                        }};
                } else {
                    occurencesPath = DataDir.value() + "/clone_candidates.csv";
                }
                std::cerr << "Loading clone occurences from " << occurencesPath << "..." << std::endl;
                FolderCloneOccurencesLoader{occurencesPath, [this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & rootDir, unsigned numFiles){
                        Commit * c = commits_[commitId];
                        Project * p = projects_[projectId];
                        CloneOriginal * co = nullptr; 
                        assert(p != nullptr);
                        assert(c != nullptr);
                        if (! IgnoreFolderOriginals.value()) {
                            co = cloneOriginals_[cloneId];
                            assert(co != nullptr);
                        }
                        std::string root = rootDir;
                        c->addedClones.insert(std::make_pair(rootDir, co));
                    }};
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        // add the commit to the project
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        fileOriginals_[contentsId].updateWith(p, c, pathId, paths_);
                    }};
                std::cerr << "    " << fileOriginals_.size() << " unique contents" << std::endl;
            }


            /** Calculates the time summaries of clones.
             */
            void calculateTimes() {
                std::cerr << "Summarizing projects..." << std::endl;
                std::vector<std::thread> threads;
                std::vector<std::unordered_map<uint64_t, Stats>> stats;

                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride) {
                    stats.push_back(std::unordered_map<uint64_t, Stats>());
                    threads.push_back(std::thread([stride, &stats, & completed, this]() {
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size()) 
                                    break;
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            summarizeProject(p, stats[stride]);
                        }
                        {
                            std::lock_guard<std::mutex> g(mWriteBack_);
                            for (auto i : stats[stride])
                                clonesOverTime_[i.first] += i.second;
                        }
                    }));
                }
                for (auto & i : threads)
                    i.join();
                std::cout << "    " << clonesOverTime_.size() << " distinct times..." << std::endl;
            }

            void output() {
                std::string suffix = ".csv";
                if (IgnoreFolderOriginals.value())
                    suffix = ".ignoredOriginals.csv";
                std::cerr << "Writing results..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/clonesOverTime" + suffix);
                    f << "#time,projects,files,npmFiles,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
                    Stats x;
                    for (auto i : clonesOverTime_) {
                        x += i.second;
                        f << i.first << "," <<
                            x.projects << "," <<
                            x.files << "," <<
                            x.npmFiles << "," <<
                            x.clones << "," <<
                            x.npmClones << "," <<
                            x.folderClones << "," <<
                            x.npmFolderClones << "," <<
                            x.changedFolderClones << "," <<
                            x.npmChangedFolderClones << std::endl;
                    }
                    std::cerr << "Done." << std::endl;
                }
                std::cerr << "Writing project results..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/projectsCloneSummary" + suffix);
                    f << "#projectId,changes,npmChanges,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
                    for (Project * p : projects_) {
                        if (p == nullptr)
                            continue;
                        f << p->id << "," <<
                            p->stats.files << "," <<
                            p->stats.npmFiles << "," <<
                            p->stats.clones << "," <<
                            p->stats.npmClones << "," <<
                            p->stats.folderClones << "," <<
                            p->stats.npmFolderClones << "," <<
                            p->stats.changedFolderClones << "," <<
                            p->stats.npmChangedFolderClones << std::endl;
                    }
                    std::cerr << "Done." << std::endl;
                }
            }

        private:

            friend class Project;

            uint64_t convertTime(uint64_t time) {
                return time - (time % Threshold.value());
            }

            void summarizeProject(Project * p, std::unordered_map<uint64_t, Stats> & stats) {
                // create time snapshots
                std::map<uint64_t, TimeSnapshot> times;
                for (Commit * c : p->commits)
                    times[c->time];
                // iterate over the project's commits
                CommitForwardIterator<Project,Commit,CommitSnapshot> ci(p, [&,this](Commit * c, CommitSnapshot & state) {
                        state.updateWith(p, c, fileOriginals_, paths_);
                        uint64_t maxTime = 0;
                        for (Commit * child : c->children)
                            if (child->time > maxTime)
                                maxTime = child->time;
                        if (maxTime == 0)
                            maxTime = std::numeric_limits<uint64_t>::max();
                        auto t = times.find(c->time), e = times.end();
                        do {
                            t->second.updateWith(state);
                            ++t;
                        } while (t != e && t->first < maxTime);
                        return true;
                    });
                ci.process();
                // now calculate deltas and store them in the global delta map
                Stats last;
                // increase the number of projects
                ++stats[times.begin()->first].projects;
                // calculate diff for every time of the project and update the thread local diffs
                for (auto i : times) {
                    Stats current = i.second.getStats(paths_);
                    stats[convertTime(i.first)] += (current - last);
                    last = current;
                }
            }

            /* All projects. */
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            
            /** If true then given path is NPM path.
             */
            std::vector<std::pair<std::string, bool>> paths_;
            
            std::unordered_map<unsigned, FileOriginalInfo> fileOriginals_;
            std::vector<CloneOriginal *> cloneOriginals_;

            std::map<uint64_t, Stats> clonesOverTime_;

            std::mutex mCerr_;
            std::mutex mWriteBack_;
        }; // TimeAggregator
        
    } // anonymous namespace

    void ClonesOverTime(int argc, char * argv[]) {
        Threshold.updateDefaultValue(24 * 3600);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.addOption(Threshold);
        Settings.addOption(IgnoreFolderOriginals);
        Settings.parse(argc, argv);
        Settings.check();

        TimeAggregator a;
        a.initialize();
        a.calculateTimes();
        a.output();
    }
    
} // namespace dejavu
