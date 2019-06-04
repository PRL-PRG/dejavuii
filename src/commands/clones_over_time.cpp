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
                files(0), npmFiles(0), clones(0), npmClones(0), folderClones(0), npmFolderClones(), changedFolderClones(0), npmChangedFolderClones(0) {
            }

            Stats operator + (Stats const & other) const {
                Stats result;
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

            Stats diff;
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
                    }};
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
                std::cerr << "Calculating live projects..." << std::endl;
                // TODO




                std::cerr << "ordering the commits by time" << std::endl;
                std::map<unsigned, std::vector<Commit *>> commits;
                for (auto i : commits_)
                    if (i != nullptr)
                        commits[i->time].push_back(i);
                std::cerr << "    " << commits.size() << " distinct times" << std::endl;

                // and output the information
                std::cerr << "Writing..." << std::endl;
                std::ofstream f(DataDir.value() + "/clones_over_time.csv");
                f << "#time,projects, files,npmFiles,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
                Stats x;
                for (auto i : commits) {
                    for (Commit * c : i.second)
                        x += c->diff;
                    f << i.first << ",0," <<
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

            bool isFirstOccurence(unsigned contents) {
                auto i = contents_.find(contents);
                if (i != contents_.end())
                    return false;
                contents_.insert(contents);
                return true;
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
            std::unordered_set<unsigned> contents_;
        }; // TimeAggregator


        /** Contains information about active project files.
         */
        class ProjectState {
        public:

            /** Information about a single path.

                Determines whether the given file is a clone, folder clone, or changed folder clone. Also keeps the contents of the file so that we can determine which (if any) record from parent commit should be preserved in merge commits. 
             */
            class PathStats {
            public:
                bool clone;
                bool folderClone;
                bool changedFolderClone;
                size_t contents;

                PathStats():
                    clone(false),
                    folderClone(false),
                    changedFolderClone(false),
                    contents(0) {
                }
            }; // ProjectState::PathStats


            /** A map from path ids to path stats for all active files.
             */
            std::unordered_map<unsigned, PathStats> paths;

            ProjectState() {
                
            }

            ProjectState(ProjectState const & other) {
                for (auto i : other.paths)
                    paths[i.first] = i.second;
            }

            /** Merges with previous commit state.

                Due to the fact that a merge commit is required to change any of files not being identical in all of its parents, the merge is only concerned with files where the merge preserves values of one of the parent commits since this "change" is only virtual and should only copy the state from the partricular parent.
             */
            void mergeWith(ProjectState const & state, Commit * c) {
                assert(c != nullptr);
                // get all paths in the parent commit
                for (auto i : state.paths) {
                    // find if the path is a change in current commit
                    auto own = c->changes.find(i.first);
                    // if there is a change to the file, check if the change is to same contents and if so just copy the original state to the current state, otherwise does nothing 
                    if (own != c->changes.end()) {
                        if (own->second == i.second.contents)
                            paths[i.first] = i.second;
                    // otherwise we have a file in parent which is not changed in current commit, so just copy the parent file as well
                    } else {
                        paths[i.first] = i.second;
                    }
                }
            }

            /** Records given change to the file stats.
             */
            void change(unsigned path, unsigned contents, TimeAggregator * ta) {
                // if the file is to be deleted, just delete the file from the paths and exit
                if (contents == FILE_DELETED) {
                    paths.erase(path);
                    return;
                }
                // otherwise obtain the stats
                PathStats & s = paths[path];
                // if the contents won't be changed, then the files was altready dealth with in the merge function so we just return
                if (s.contents == contents)
                    return;
                // otherwise it is regular change, update the contents and fill in the clone details
                s.contents = contents;
                // update the file clone info, i.e. have we seen the file before?
                if (!ta->isFirstOccurence(contents)) 
                    s.clone = true;
                // if the file is marked as folder clone, mark it as changed folder clone
                if (s.folderClone)
                    s.changedFolderClone = true;
                // note that folder clones are dealt with later in markAsFolderClone function
            }


            void markAsFolderClone(Commit * c, std::string const & rootDir, TimeAggregator * ta) {
                for (auto i : c->changes) {
                    if (i.second == FILE_DELETED)
                        continue;
                    unsigned path = i.first;
                    if (ta->paths_[path].first.find(rootDir) == 0) {
                        PathStats & ps = paths[path];
                        assert(ps.contents != 0);
                        ps.folderClone = true;
                        ps.changedFolderClone = false;
                    }
                }
            }

            Stats createStats(TimeAggregator * ta) {
                Stats ti;
                for (auto i : paths) {
                    ++ti.files;
                    if (i.second.clone)
                        ++ti.clones;
                    if (i.second.folderClone)
                        ++ti.folderClones;
                    if (i.second.changedFolderClone)
                        ++ti.changedFolderClones;
                    if (ta->paths_[i.first].second) {
                        ++ti.npmFiles;
                        if (i.second.clone)
                            ++ti.npmClones;
                        if (i.second.folderClone)
                            ++ti.npmFolderClones;
                        if (i.second.changedFolderClone)
                            ++ti.npmChangedFolderClones;
                    } 
                }
                return ti;
            }
        };

        /** Summarizes clones information for given project.
         */
        void Project:: summarizeClones(TimeAggregator * ta) {

            std::unordered_map<Commit *, Stats> commitStats;
            
            CommitForwardIterator<Commit, ProjectState, true> ci([&,this](Commit * c, ProjectState & state) {
                    assert(c != nullptr);
                    // update the state according to changes in the given commit
                    for (auto i : c->changes) {
                        state.change(i.first, i.second, ta);
                    }
                    // look at any folder clones introduced by the commit and mark the files as folder clones if they are not the original
                    for (auto i : c->introducedClones) {
                        Clone * co = i.second;
                        // if the clone is its own original, skip it
                        if (co->isOriginal(this, c, i.first))
                            continue;
                        // otherwise get all files that belong to the clone and mark them as folder clones
                        state.markAsFolderClone(c, i.first, ta);
                        
                    }
                    // create stats for the commit and store them in the stats map for the project
                    Stats cs = state.createStats(ta);
                    assert(cs.files == state.paths.size());
                    commitStats[c] = cs;
                    // calculate the diff and update the commit diff accordingly
                    if (c->parents.empty()) {
                        c->diff += cs;
                    } else {
                        Stats pStats;
                        for (auto p : c->parents) {
                            //assert(p->time <= c->time);
                            pStats += commitStats[p];
                        }
                        c->diff += (cs - pStats);
                    }
                    return true;
                });
            // add initial commits
            for (Commit * c : commits)
                if (c->numParentCommits() == 0)
                    ci.addInitialCommit(c);
            // process the project
            ci.process();
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
