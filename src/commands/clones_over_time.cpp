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
        };

        
        /*
        class CloneOccurence {
        public:
            // the occurence specification
            Project * project;
            Commit * commit;
            std::string path;

            CloneOccurence(Project * p, Commit * c, std::string const & path):
                project(p),
                commit(c),
                // make sure there is always trailing / so that we do not match subexpressions but only whole dirs
                path(path + "/") {
            }

            /** Returns true if the clone contains given path.
             *  /
            bool contains(std::string const & path) {
                return path.find(this->path) == 0; 
            }

            }; */

        class CloneOriginal {
        public:
            Project * project;
            Commit * commit;
            std::string root;

            CloneOriginal(Project * p, Commit * c, std::string const & path):
                project(p),
                commit(c),
                root(path) {
            }

            bool isOriginal(Project * p, Commit * c, std::string const & path) {
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
                assert(clone);
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
                    pi.contentsId = i.second;
                    pi.clone = isClone(i.second, i.first, p, c, fileOriginals);
                    if (pi.folderClone)
                        pi.changedFolderClone = true;
                }
                // finally, if the commit introduces any folder clones, we must mark them as such
                for (auto clone : c->addedClones) {
                    std::string const & root = clone.first;
                    // We don't have to maintain project with folder clones, because if there is a clone in commit, then the clone happens in *all* projects containing the clone. The only exception is if the original is itself a clone and happens to be in the oldest of projects containing the clone, which is what we check here (or if the original is same project, same commit, different path), both are degenerate cases
                    if (clone.second->isOriginal(p, c, root))
                        continue;
                    for (auto i : c->changes) {
                        if (paths[i.first].first.find(root) == 0) {
                            if (!files_[i.first].clone) {
                                std::cerr << "Expected clone: " << p->id << "," << c->id << "," << i.first << "," << i.second << std::endl;
                                std::cerr << "Original: " << fileOriginals[i.second].project->id << ","  << fileOriginals[i.second].commit->id << "," << fileOriginals[i.second].pathId << std::endl;
                                std::cerr << "root: " << root << std::endl;
                            }
                            files_[i.first].setAsFolderClone();
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
                    size_t index = (static_cast<size_t>(i.first) << 32) + i.second.contentsId;
                    auto j = files_.find(index);
                    if (j == files_.end())
                        files_.insert(std::make_pair(index, static_cast<PathInfo>(i.second)));
                }
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
                        std::string p = std::string("/" + path);
                        paths_[id] = std::make_pair(p, IsNPMPath(path));
                    }};
                std::cerr << "    " << paths_.size() << " paths loaded" << std::endl;
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
                std::cerr << "Loading clone originals..." << std::endl;
                FolderCloneOriginalsLoader{[this](unsigned cloneId, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path, bool isOriginalClone){
                        Commit * c = commits_[commitId];
                        Project * p = projects_[projectId];
                        if (cloneOriginals_.size() <= cloneId)
                            cloneOriginals_.resize(cloneId + 1);
                        cloneOriginals_[cloneId] = new CloneOriginal(p, c, path);
                    }};
                std::cerr << "Loading clone occurences..." << std::endl;
                FolderCloneOccurencesLoader{[this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & rootDir, unsigned numFiles){
                        Commit * c = commits_[commitId];
                        Project * p = projects_[projectId];
                        CloneOriginal * co = cloneOriginals_[cloneId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        assert(co != nullptr);
                        c->addedClones.insert(std::make_pair(rootDir, co));
                    }};
            }


            /** Calculates the time summaries of clones.
             */
            void calculateTimes() {
                std::cerr << "Summarizing projects..." << std::endl;
                std::vector<std::thread> threads;
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, this]() {
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size())
                                    return;
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            summarizeProject(p);
                        }
                    }));
                for (auto & i : threads)
                    i.join();

                
                #ifdef HAHA
                // first initialize TimeInfos for all commit times
                // now, add partial results from each project to the summary
                std::cerr << "Summarizing projects..." << std::endl;
                size_t i = 0;
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    p->summarizeClones(this);
                    std::cerr << (i++) << "\r";
                }
                std::cerr << "    " << projects_.size() << " projects analyzed..." << std::endl;
                // finally create live project counts
                std::cerr << "    " << stats_.size() << " distinct times" << std::endl;

                // and output the information
                std::cerr << "Writing..." << std::endl;
                std::ofstream f(DataDir.value() + "/clones_over_time.csv");
                f << "#time,projects,files,npmFiles,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
                Stats x;
                for (auto i : stats_) {
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
                #endif
            }

        private:

            friend class Project;


            void summarizeProject(Project * p) {
                // create time snapshots
                std::unordered_map<uint64_t, TimeSnapshot> times;
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
                
            }

            /* All projects. */
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            
            /** If true then given path is NPM path.
             */
            std::vector<std::pair<std::string, bool>> paths_;
            
            std::unordered_map<unsigned, FileOriginalInfo> fileOriginals_;
            std::vector<CloneOriginal *> cloneOriginals_;

            std::map<unsigned, Stats> stats_;

            std::mutex mCerr_;
        }; // TimeAggregator


        #ifdef HAHA


        /** Holds information about all files and their state at any given commit.
         */
        class CommitSnapshot {
        public:
            /** path id -> contents id *and* stats
             */
            std::unordered_map<unsigned, std::pair<unsigned, PathStats>> paths;

            CommitSnapshot() {
                
            }

            CommitSnapshot(CommitSnapshot const & other) {
                for (auto i : other.paths)
                    paths[i.first] = i.second;
            }

            /** Merges two commit states (called for merge commits).

                When we have a merge commit, we must make sure that the commit snapshot contains all relevant files, i.e. all files active in children modulo the files deleted by the merge commit. This is done by the applyChanges method.

                However, when a merge commit simply changes the path to a value from one of its parents, then that value's properties should be kept intact (it's not really a change since new file has not been introduced). This is the job of this method:
             */
            void mergeWith(CommitSnapshot const & state, Commit * c) {
                assert(c != nullptr);
                // get all paths in the parent commit
                for (auto i : state.paths) {
                    // find if the path is a change in current commit
                    auto own = c->changes.find(i.first);
                    // if there is a change to the file, check if the change is to same contents and if so just copy the original state to the current state, otherwise does nothing 
                    if (own != c->changes.end()) {
                        if (own->second == i.second.first)
                            paths[i.first] = i.second;
                    // otherwise we have a file in parent which is not changed in current commit, so just copy the parent file as well
                    } else {
                        paths[i.first] = i.second;
                    }
                }
            }

            /** Applies the changes from the given commit to the snapshot.
             */
            void applyChanges(Project * p, Commit * c, TimeAggregator * ta) {
                // first update the state according to the changes in the commit
                for (auto i : c->changes) {
                    // if the file is to be deleted, simply delete the record 
                    if (i.second == FILE_DELETED) {
                        paths.erase(i.first);
                    // otherwise update the record accordingly
                    } else {
                        auto & j = paths[i.first];
                        // if the contents of the file is identical to the one the file is to be changed to, ignore it (it has been dealt with in the mergeWith method)
                        if (j.first == i.second)
                            continue;
                        j.first = i.second;
                        j.second.clone = ! ta->isFirstFileOccurence(p, c, i.second);
                        if (j.second.folderClone)
                            j.second.changedFolderClone = true;
                    }
                }
                // now check any introduced clones and mark their files as folder clones
                for (auto i : c->addedClones) {
                    Clone * co = i.second;
                    // if current project, commit and root dir are the original, don't do anything
                    if (co->isOriginal(p, c, i.first))
                        continue;
                    // otherwise walk the changed files and if they belong to the clone folder, mark them as clone folder changes
                    for (auto j : c->changes) {
                        if (j.second == FILE_DELETED)
                            continue;
                        if (ta->paths_[j.first].first.find(i.first) == 0) {
                            auto & s = paths[j.first];
                            // if the file is to be folder clone, then the following must be true
                            // either the file is a clone
                            // or the same file contents is being added multiple times and all but one of the paths is clone
                            if (! s.second.clone) {
                                unsigned count = 0;
                                unsigned clones = 0;
                                for (auto x : c->changes)
                                    if (x.second == j.second) {
                                        ++count;
                                        if (paths[x.first].second.clone == true)
                                            ++clones;
                                    }
                                if (clones + 1 != count) {
                                    std::cerr << "Count: " << count << std::endl;
                                    std::cerr << "Project: " << p->id << std::endl;
                                    std::cerr << "Commit: " << c->id << std::endl;
                                    std::cerr << "File path: " << j.first << " -- " << ta->paths_[j.first].first << std::endl;
                                    auto o =  ta->originals_[j.second];
                                    std::cerr << "Original: " << o.first->id << ", " << o.second->id << std::endl;
                                }
                                assert(clones + 1 == count);
                            }
                            //                            assert(s.second.clone); // if it is folder clone, it *has* to be file clone as well
                            s.second.folderClone = true;
                            s.second.changedFolderClone = false;
                        }
                    }
                }
            }
        };


        /** Generalization of a commit snapshot for a particular time.

            The time snapshot takes into account the possibility of two different versions of the same file (by path) existing at the same time. This can happen when two "branches" existed at the time and while some files were similar, some were changed only in first and others only in second (or indeed both).

            The project can then be characterized by diffs between two neighbouring time snapshots, which can then be easily combined into the global timeline. 
         */
        class TimeSnapshot {
        public:

            /** path id -> [ contents id -> stats ]

                i.e. for each path contains a set of states active at given time (states are recognized by the contents of the file)
             */
            std::unordered_map<unsigned, std::unordered_map<unsigned, PathStats>> paths;

            void addCommit(Commit * c, CommitSnapshot const & cs) {
                for (auto i : cs.paths) {
                    std::unordered_map<unsigned, PathStats> & x = paths[i.first];
                    auto j = x.find(i.second.first); // the contents
                    if (j == x.end())
                        x.insert(i.second);
                }
            }

            Stats stats(TimeAggregator * ta) {
                Stats result;
                for (auto i : paths) {
                    bool npm = ta->paths_[i.first].second;
                    for (auto j : i.second) {
                        PathStats const & ps = j.second;
                        ++result.files;
                        if (ps.clone) 
                            ++result.clones;
                        if (ps.folderClone)
                            ++result.folderClones;
                        if (ps.changedFolderClone)
                            ++result.changedFolderClones;
                        if (npm) {
                            ++result.npmFiles;
                            if (ps.clone) 
                                ++result.npmClones;
                            if (ps.folderClone)
                                ++result.npmFolderClones;
                            if (ps.changedFolderClone)
                                ++result.npmChangedFolderClones;
                        }
                        if (ps.changedFolderClone)
                            assert(ps.folderClone);
                    }
                }
                return result;
            }
        };

        

        /** Summarizes clones information for given project.
         */
        void Project:: summarizeClones(TimeAggregator * ta) {

            std::unordered_map<Commit *, CommitSnapshot> commitStats;

            std::map<size_t, TimeSnapshot> timeSnapshots;

            for (Commit * c : commits)
                timeSnapshots[c->time];
            //            std::cerr << timeSnapshots.size() << " -- " << commits.size() <<  std::endl;

            
            CommitForwardIterator<Project, Commit, CommitSnapshot> ci(this, [&,this](Commit * c, CommitSnapshot & state) {
                    assert(c != nullptr);
                    assert(commits.find(c) != commits.end());
                    // create the state
                    state.applyChanges(this, c, ta);
                    // create the backup
                    commitStats[c] = state;
                    // add changes to the time snapshot
                    auto i = timeSnapshots.find(c->time);
                    assert(i != timeSnapshots.end());
                    i->second.addCommit(c, state);
                    unsigned validChildren = 0;
                    for (Commit * child : c->children) {
                        if (commits.find(child) == commits.end())
                            continue;
                        ++validChildren;
                        auto j = i;
                        ++j;
                        while (j->first < child->time) {
                            j->second.addCommit(c,state);
                            ++j;
                        }
                    }
                    if (validChildren == 0) {
                        auto e = timeSnapshots.end();
                        while (++i != e)
                            i->second.addCommit(c, state);
                    }
                    return true;
                });
            // add initial commits
            /*            for (Commit * c : commits)
                if (c->numParentCommits() == 0)
                ci.addInitialCommit(c); */
            // process the project
            ci.process();

            
            Stats stats;
            for (auto i = timeSnapshots.begin(), e = timeSnapshots.end(); i != e; ++i) {
                Stats x = i->second.stats(ta);
                Stats diff = x - stats;
                ta->stats_[i->first] += diff;
                stats = x;
            }
            // increase the number of projects
            ta->stats_[timeSnapshots.begin()->first].projects += 1;

        }
#endif

        
    } // anonymous namespace


    /** TODO:

        - create the tinfos
        - make commit iterator have an extra event when commit has no children
        - be sure to detect if a clone candidate is not the clone original, in which case we don't count it

     */
    void ClonesOverTime(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        TimeAggregator a;
        a.initialize();
        a.calculateTimes();
        
    }
    

    
} // namespace dejavu
