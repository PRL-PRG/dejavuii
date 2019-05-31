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

        class Commit {
        public:
            size_t id;
            uint64_t time;
            unsigned numParents;
            std::unordered_map<unsigned, unsigned> changes;
            std::vector<Commit *> children;

            // implementation for the commit iterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }

            std::unordered_map<std::string, Clone*> introducedClones;

            unsigned numParentCommits() const {
                return numParents;
            }

            Commit(unsigned id, uint64_t time):
                id(id),
                time(time),
                numParents(0) {
            }

            void addParent(Commit * p) {
                ++numParents;
                p->children.push_back(this);
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



        /** Information about aggregated numbers of different files at a time.

            The times for which TimeInfo is provided are discrete and reflect the situation *after* all commits happening at the particular time. 
         */
        class TimeInfo {
        public:
            
            /** Time for which the time info is valid.
             */
            size_t time;
            
            /** Number of projects captured at the given time.

                NOTE: This is not *live* projects, since we do not track project deletions, this just means the number of projects created before or at the date of the timeinfo. 
             */
            size_t projects;

            /** How many commits contributed to the stats.

                This should be identical to # of projects if there are no branches in the commits, but if a project has two active commits at the time (presumably these will be merged some time later, or can belong to different branches if this is supported), this will be reflected in this number:
             */
            size_t contributingCommits;

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

            TimeInfo():
                projects(0), contributingCommits(0), files(0), npmFiles(0), clones(0), npmClones(0), folderClones(0), npmFolderClones(), changedFolderClones(0), npmChangedFolderClones(0) {
            }

            /** Adds the other timeinfo stats to itself, increasing the contributing commits by 1.
             */
            TimeInfo & operator += (TimeInfo const & other) {
                assert(other.contributingCommits == 1);
                ++contributingCommits;
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
        };
        

        /** Aggregates the number of clones over time.

            For each valid time (i.e. a time of a commit) calculates the information about the number of projects, commits, files, npm_files, cloned files and files in cloned folder.

            While this is easy on a per project basis, the aggregation requires a bit of thought about how to deal with commit times that are skipped - i.e. assume two projects A and B with commits at the following times:

            A:   C1   C2                         C3      C4     C5
            B:   C6          C7    C8            C9

            ---> time

            t(C1,C6) is simple because at this particular time, we can just snapshot the C1 and C6 state and we have the correct information. but for instance the t(C2) can't just be obtained by snapshoting C2, because the files from C6 are still alive.

            The algorithm solves this by each commit looking at its predecessor(s) and copying them to all times up to its own time, i.e. in the case above when we get to process C7, we copy the results of C6 also to time t(C2).

            Similarly, we must make sure that any last commit to a project will be copied to all subsequent times, so results of C9 will be copied to t(C4) and t(C5). 
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
                        clones_[id] = new Clone(id, numFiles, project, commit, rootDir);
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


            void calculateTimes() {
                
            }

        private:


            /** Adds the given TimeInfo partial results to all times from the ti's time to the specified time (both times exclusive).
             */
            void addUntil(size_t time, TimeInfo const & ti) {
                auto e = timeInfo_.end();
                auto i = timeInfo_.find(ti.time);
                assert(i != e);
                ++i;
                while (i != e && i->second.time < time) {
                    i->second += ti;
                    ++i;
                }
            }

            /** Adds the given time info to the specified time.
             */
            void add(size_t time, TimeInfo const & ti) {
                auto i = timeInfo_.find(ti.time);
                assert(i != timeInfo_.end());
                i->second += ti;
            }
            

            /* All projects. */
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            std::vector<std::string> paths_;
            std::vector<Clone*> clones_;
            /** Already seen file contents so that we can determine whether a change makes file a clone or not.
             */
            std::unordered_set<unsigned> contents_;


            /** Ordered map of times and their summaries.
             */
            std::map<size_t, TimeInfo> timeInfo_;
            
        }; // Analyzer
        
    } // anonymous namespace


    /**


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
