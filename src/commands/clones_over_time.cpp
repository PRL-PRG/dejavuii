#include <unordered_map>
#include <map>
#include <vector>

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
                result.npmClones = npmClones + other.npmFiles;
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
                result.npmClones = npmClones - other.npmFiles;
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
                npmClones += other.npmFiles;
                folderClones += other.folderClones;
                npmFolderClones += other.npmFolderClones;
                changedFolderClones += other.changedFolderClones;
                npmChangedFolderClones += other.npmChangedFolderClones;
                return *this;
            }

            
            
        }; // Stats

        class Commit {
        public:
            size_t id;
            uint64_t time;
            std::unordered_map<unsigned, unsigned> changes;
            std::vector<Commit *> children;
            std::vector<Commit *> parents;

            // implementation for the commit iterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }

            std::unordered_map<std::string, Clone*> introducedClones;

            unsigned numParentCommits() const {
                return parents.size();
            }

            Commit(unsigned id, uint64_t time):
                id(id),
                time(time) {
            }

            void addParent(Commit * p) {
                p->children.push_back(this);
                parents.push_back(p);
            }

            void addChange(unsigned pathId, unsigned contentsId) {
                changes.insert(std::make_pair(pathId, contentsId));
            }

        };

        class CloneOccurence {
        public:
            // the occurence specification
            Project * project;
            Commit * commit;
            std::string path;
            // the clone id
            Clone * clone;

            CloneOccurence(Project * p, Commit * c, std::string const & path, Clone * clone):
                project(p),
                commit(c),
                // make sure there is always trailing / so that we do not match subexpressions but only whole dirs
                path(path + "/"),
                clone(clone) {
            }

            bool contains(std::string const & path) {
                return path.find(this->path) == 0; 
            }

        };

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;

            std::unordered_set<Commit *> commits;

            bool hasCommit(Commit * c) const {
                return commits.find(c) != commits.end();
            }

            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt) {
            }

            void addCommit(Commit * c) {
                commits.insert(c);
            }

            void summarizeClones(TimeAggregator * ta);

            bool shouldIgnore() {
                for (auto c : commits)
                    for (auto p : c->parents)
                        if (p->time > c->time)
                            return true;
                return false;
            }

        };

        /** Information about a clone.
         */
        class Clone {
        public:
            size_t id;
            Project * originalProject;
            Commit * originalCommit;
            std::string originalRoot;


            Clone(unsigned id, Project * project, Commit * commit, std::string const & rootDir):
                id(id),
                originalProject(project),
                originalCommit(commit),
                originalRoot(rootDir) {
            }

            /** Returns true if the original of the clone is the provided project, clone and root dir combination, false otherwise.
             */
            bool isOriginal(Project * p, Commit * c, std::string const & rootDir) {
                return originalCommit == c && originalProject == p && originalRoot == rootDir;
            }
        }; // Clone

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
                        paths_[id] = std::make_pair(p, IsNPMPath(p));
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
                        auto i = originals_.find(contentsId);
                        if (i == originals_.end()) {
                            originals_.insert(std::make_pair(contentsId, std::make_pair(p, c)));
                        } else {
                            if (c->time < i->second.second->time) {
                                i->second = std::make_pair(p, c);
                            }
                        }
                            
                    }};
                std::cerr << "    " << originals_.size() << " unique contents" << std::endl;
                // now thge basic data has been loaded, load the clone originals
                std::cerr << "Loading clone originals..." << std::endl;
                FolderCloneOriginalsLoader{[this](unsigned id, unsigned numFiles, unsigned projectId, unsigned commitId, std::string const & rootDir) {
                        if (clones_.size() <= id)
                            clones_.resize(id + 1);
                        Project * project = projects_[projectId];
                        Commit * commit = commits_[commitId];
                        clones_[id] = new Clone(id, project, commit, rootDir + "/");
                    }};
                std::cerr << "    " << clones_.size() << " unique clones loaded" << std::endl;
                std::cerr << "Loading clones..." << std::endl;
                size_t missingClones = 0;
                FolderClonesLoader{[this, &missingClones](unsigned projectId, unsigned commitId, std::string const & rootDir, unsigned numFiles, unsigned cloneId){
                        Commit * commit = commits_[commitId];
                        Clone * clone = clones_[cloneId];
                        if (clone == nullptr) {
                            ++missingClones;
                            return;
                        }
                        assert(clone != nullptr);
                        auto i = commit->introducedClones.find(rootDir);
                        if (i != commit->introducedClones.end()) 
                            assert(i->second == clone && "Same commit must have same clones in same directory");
                        else
                            commit->introducedClones[rootDir] = clone;
                    }};
                std::cerr << "    " << missingClones << " missing clones" << std::endl;
            }


            /** Calculates the time summaries of clones.
             */
            void calculateTimes() {
                // first initialize TimeInfos for all commit times
                // now, add partial results from each project to the summary
                std::cerr << "Summarizing projects..." << std::endl;
                size_t i = 0;
                size_t ignored = 0;
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    if (p->shouldIgnore()) {
                        ++ignored;
                        continue;
                    }
                    p->summarizeClones(this);
                    std::cerr << (i++) << "\r";
                }
                std::cerr << "    " << projects_.size() << " projects analyzed..." << std::endl;
                std::cerr << "    " << ignored << " projects ignored..." << std::endl;
                // finally create live project counts
                std::cerr << "    " << stats_.size() << " distinct times" << std::endl;

                // and output the information
                std::cerr << "Writing..." << std::endl;
                std::ofstream f(DataDir.value() + "/clones_over_time.csv");
                f << "#time,projects, files,npmFiles,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
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
            }

        private:

            friend class Project;
            friend class ProjectState;
            friend class CommitSnapshot;
            friend class TimeSnapshot;

            bool isFirstFileOccurence(Project * p, Commit * c, unsigned contents) {
                if (visited_.find(contents) != visited_.end())
                    return false;
                auto i = originals_[contents];
                if (i.first == p && i.second == c) {
                    visited_.insert(contents);
                    return true;
                }
                return false;
            }

            

            /* All projects. */
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            
            /** If true then given path is NPM path.
             */
            std::vector<std::pair<std::string, bool>> paths_;
            std::vector<Clone*> clones_;
            
            /** Already seen file contents so that we can determine whether a change makes file a clone or not.

                We don't really care about who is the original in the summaries, so first occurence is fine. 
             */
            std::unordered_set<unsigned> visited_;
            std::unordered_map<unsigned, std::pair<Project *, Commit *>> originals_;

            std::map<unsigned, Stats> stats_;
        }; // TimeAggregator


        /** Statistics for any given path.

            Holds information whether the file contents is original, or a clone, whether it belongs to a folder clone, and whether it has been updated since the folder clone was established. 
         */
        class PathStats {
        public:
            bool clone;
            bool folderClone;
            bool changedFolderClone;

            PathStats():
                clone(false),
                folderClone(false),
                changedFolderClone(false) {
            }
        };

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
                        j.second.clone = ta->isFirstFileOccurence(p, c, i.second);
                        if (j.second.folderClone)
                            j.second.changedFolderClone = true;
                    }
                }
                // now check any introduced clones and mark their files as folder clones
                for (auto i : c->introducedClones) {
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
                            if (ps.clone) 
                                ++result.npmClones;
                            if (ps.folderClone)
                                ++result.npmFolderClones;
                            if (ps.changedFolderClone)
                                ++result.npmChangedFolderClones;
                        }
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

            
            CommitForwardIterator<Project, Commit, CommitSnapshot, true> ci(this, [&,this](Commit * c, CommitSnapshot & state) {
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
            for (Commit * c : commits)
                if (c->numParentCommits() == 0)
                    ci.addInitialCommit(c);
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


        
    } // anonymous namespace


    /** TODO:

        - create the tinfos
        - make commit iterator have an extra event when commit has no children
        - be sure to detect if a clone candidate is not the clone original, in which case we don't count it

     */
    void ClonesOverTime(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        TimeAggregator a;
        a.initialize();
        a.calculateTimes();
        
    }
    

    
} // namespace dejavu
