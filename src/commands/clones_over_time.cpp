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
            size_t files;

            /** Number of files that are in the node_modules folder.
             */
            size_t npmFiles;

            /** Number of files that are clones, i.e. files whose contents we have already seen elsewhere.
             */
            size_t clones;

            /** Number of files that we have seen elsewhere that reside in node_modules folder.
             */
            size_t npmClones;

            /** Number of files which belong to a folder clone and have not been changed.
             */
            size_t folderClones;

            /** Number of unchanged files in node_modules directories belonging to a folder clone.
             */
            size_t npmFolderClones;

            /** Number of files belonging to a folder clone that have been changed since cloned.
             */
            size_t changedFolderClones;

            /** Number of files belonging to a node_modules folder which are part of a folder clone, but have been changed since they were cloned.
             */
            size_t npmChangedFolderClones;

            Stats():
                files(0), npmFiles(0), clones(0), npmClones(0), folderClones(0), npmFolderClones(), changedFolderClones(0), npmChangedFolderClones(0) {
            }
        }; // Stats

        class StatsDiff {
        public:
            Stats additions;
            Stats deletions;
        };

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

            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt) {
            }

            void addCommit(Commit * c) {
                commits.insert(c);
            }

            void summarizeClones(TimeAggregator * ta);


            std::unordered_map<Commit*, StatsDiff> stats;
            
        private:
            

        };

        /** Information about a clone.
         */
        class Clone {
        public:
            size_t id;
            size_t numFiles;
            Project * originalProject;
            Commit * originalCommit;
            std::string originalRoot;

            /** Number of projects the clone has appeared. 
             */
            size_t weightProjects;

            /** Number of total occurences of the clone. 
             */
            size_t weight;

            /** Number of different roots the original is cloned to. 
             */
            size_t weightRoots;

            Clone(unsigned id, unsigned numFiles, Project * project, Commit * commit, std::string const & rootDir):
                id(id),
                numFiles(numFiles),
                originalProject(project),
                originalCommit(commit),
                originalRoot(rootDir) {
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
                        paths_[id] = std::string("/") + path;
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
                        clones_[id] = new Clone(id, numFiles, project, commit, rootDir + "/");
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
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    p->summarizeClones(this);
                    std::cerr << (i++) << "\n";
                }
                std::cerr << "    " << projects_.size() << " projects analyzed..." << std::endl;
                // finally create live project counts
                std::cerr << "Calculating live projects..." << std::endl;
                // TODO

                // and output the information
                std::cerr << "Writing..." << std::endl;
                std::ofstream f(DataDir.value() + "/clones_over_time.csv");
                f << "#time,projects,commits,files,npmFIles,clones,npmClones,folderClones,npmFolderClones,changedFolderClones,npmChangedFolderClones" << std::endl;
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
            std::vector<std::string> paths_;
            std::vector<Clone*> clones_;
            /** Already seen file contents so that we can determine whether a change makes file a clone or not.
             */
            std::unordered_set<unsigned> contents_;
        }; // TimeAggregator

        class ProjectState {
        public:
            class PathStats {
            public:
                bool npm;
                bool clone;
                bool folderClone;
                bool changedFolderClone;
                size_t contents;
            }; // ProjectState::PathStats

            
            std::unordered_map<unsigned, PathStats> paths;

            ProjectState() {
                
            }

            ProjectState(ProjectState const & other) {
                for (auto i : other.paths)
                    paths[i.first] = i.second;
            }

            /** Merges with previous commit state.

                Due to the fact that a merge commit is required to change any of files not being identical in all of its parents, the merge is only concerned with files where the merge preserves values of oen of the parent commits since this "change" is only virtual and should only copy the state from the partricular parent. 

             */
            void mergeWith(ProjectState const & state, Commit * c) {
                assert(c != nullptr);
                for (auto i : state.paths) {
                    auto j = c->changes.find(i.first);
                    if (j == c->changes.end()) {
                        // make sure the contents are the same
                        assert(i.second.contents == paths[i.first].contents);
                        // and do nothing
                        continue;
                    }
                    if (i.second.contents == j->second)
                        paths[i.first] = i.second;
                }
            }

            void change(unsigned path, unsigned contents, TimeAggregator * ta) {
                PathStats & s = paths[path];
                // it's not a change (was already dealt with in the merge phase)
                if (s.contents == contents)
                    return;
                if (contents == FILE_DELETED) {
                    paths.erase(path);
                    return;
                }
                // if this is the first time we see the path, determine whether it is NPM or not
                if (s.contents == 0)
                    s.npm = IsNPMPath(ta->paths_[path]);
                // fill in the details
                s.contents = contents;
                if (!ta->isFirstOccurence(contents)) 
                    s.clone = true;
                if (s.folderClone)
                    s.changedFolderClone = true;
            }

            void setAsFolderClone(unsigned path) {
                auto i = paths.find(path);
                assert(i != paths.end());
                i->second.folderClone = true;
                i->second.changedFolderClone = false;
            }

            Stats createStats() {
                Stats ti;
                for (auto i : paths) {
                    ++ti.files;
                    if (i.second.clone)
                        ++ti.clones;
                    if (i.second.folderClone)
                        ++ti.folderClones;
                    if (i.second.changedFolderClone)
                        ++ti.changedFolderClones;
                    if (i.second.npm) {
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
            
            CommitForwardIterator<Commit, ProjectState, true> ci([&,this](Commit * c, ProjectState & state) {
                    assert(c != nullptr);
                    // update the state according to changes in given commit
                    for (auto i : c->changes) 
                        state.change(i.first, i.second, ta);
                    // if the commit introduces any folder clones, update the state accordingly
                    for (auto i : c->introducedClones) {
                        Clone * o = i.second;
                        assert(o != nullptr);
                        // if the clone candidate is the original itself, then ignore it
                        if (o->originalProject == this && o->originalCommit == c && o->originalRoot == i.first)
                            continue;
                        for (auto ii : c->changes) {
                            if (ii.second != FILE_DELETED)
                                if (ta->paths_[ii.first].find(i.first) == 0)
                                    state.setAsFolderClone(ii.first);
                        }
                        state.createStats();
                    }
                    // create partial TimeInfo from the state, update the global state and register the time info with the commits
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
