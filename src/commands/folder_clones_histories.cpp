#include <unordered_map>
#include <vector>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


/**


 */
namespace dejavu {

    namespace {

        class Analyzer;
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
            /** number of cloned files
             */
            size_t paths;
            /** Number of changes observed to the files belonging to the cloned folder.
             */
            size_t changes;
            /** Number of deletions to the files belonging to the cloned folder. 
             */
            size_t deletions;
            /** Number of merges - i.e. we have commits merge and a file is merged, which does not come from same sources (same clone occureences), the file is deleted from both (or one if the other file is not registered with a clone) and the merge count is increased.
             */
            size_t merges;
            /** The paths that have been changed (i.e. num of unique paths changed).
             */
            std::unordered_set<unsigned> changedPaths;

            std::unordered_set<unsigned> deletedPaths;

            CloneOccurence(Project * p, Commit * c, std::string const & path, Clone * clone):
                project(p),
                commit(c),
                // make sure there is always trailing / so that we do not match subexpressions but only whole dirs
                path(path + "/"),
                clone(clone),
                paths(0),
                changes(0),
                deletions(0),
                merges(0) {
            }

            bool contains(std::string const & path) {
                return path.find(this->path) == 0; 
            }

        };

        class TrackedClonesMap {
        public:
            TrackedClonesMap() {

            }

            TrackedClonesMap(TrackedClonesMap const & from) {
                for (auto i : from.trackedPaths_)
                    trackedPaths_[i.first] = i.second;
            }

            /** Merging gets all paths from the other map and for each path does the following:

                - if the path is not found, it is copied
                - if the path is found and points to the same co, nothing is done
                - if the poth is found and points to different co, it is reported as merge
             */
            void mergeWith(TrackedClonesMap const & other) {
                for (auto i : other.trackedPaths_) {
                    auto j = trackedPaths_.find(i.first);
                    if (j == trackedPaths_.end()) {
                        trackedPaths_[i.first] = i.second;
                    } else if (i.second != j->second) {
                        ++j->second->merges;
                        j->second->changedPaths.insert(j->first);
                        trackedPaths_.erase(j);
                    }
                }
            }

            void addPath(unsigned path, CloneOccurence * co) {
                auto i = trackedPaths_.find(path);
                if (i != trackedPaths_.end())
                    deletePath(path);
                trackedPaths_[path] = co;
            }

            void deletePath(unsigned path) {
                trackedPaths_[path]->deletedPaths.insert(path);
                ++trackedPaths_[path]->deletions;
                trackedPaths_.erase(path);
            }

            void trackChange(unsigned pathId, unsigned contentsId) {
                auto i = trackedPaths_.find(pathId);
                if (i == trackedPaths_.end())
                    return;
                CloneOccurence * co = i->second;
                if (contentsId == FILE_DELETED) {
                    deletePath(pathId);
                } else {
                    ++co->changes;
                    co->changedPaths.insert(pathId);
                }
            }

        private:
            std::unordered_map<unsigned, CloneOccurence *> trackedPaths_;


        }; // TrackedClonesMap

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
                originalRoot(rootDir),
                weightProjects(0),
                weight(0),
                weightRoots(0) {
            }
        }; // Clone


        class Analyzer {
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

            /** Calculates for each clone original (i.e. a clone group) the number of unique projects its clones appear in and the number of total places the original was cloned. 
             */
            void originalsWeight() {
                std::cerr << "Calculating originals weight..." << std::endl;
                std::unordered_map<Clone*, std::unordered_set<Project*>> uniqueProjects;
                std::unordered_map<Clone*, std::unordered_set<std::string>> uniqueRoots;
                // for each project, for each clone
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    for (Commit * c : p->commits) {
                        for (auto i : c->introducedClones) {
                            Clone * clone = i.second;
                            uniqueRoots[clone].insert(i.first);
                            uniqueProjects[clone].insert(p);
                            ++clone->weight;
                        }
                    }
                }
                for (auto const & i : uniqueProjects) 
                    i.first->weightProjects = i.second.size();
                for (auto const & i : uniqueRoots)
                    i.first->weightRoots = i.second.size();
                std::cerr << "Writing originals weights..." << std::endl;
                std::ofstream f(DataDir.value() + "/folderClonesWeights.csv");
                f << "#cloneId,weight,weightProjects,weightRoots" << std::endl;
                for (Clone * c : clones_) {
                    if (c == nullptr)
                        continue;
                    f << c->id << "," << c->weight << "," << c->weightProjects << "," << c->weightRoots << std::endl;
                }
            }

            /** Generates a summary table where for each project the number of clones and the number of clone originals is reported.
             */
            void projectsSummary() {
                std::unordered_map<Project *, size_t> numOriginals;
                std::unordered_map<Project *, size_t> originalsWeight; 
                std::unordered_map<Project *, size_t> originalsWeightProjects; 
                std::unordered_map<Project *, size_t> originalsWeightRoots; 
                std::cerr << "Calculating project original summaries..." << std::endl;
                for (Clone * c : clones_) {
                    if (c == nullptr)
                        continue;
                    ++numOriginals[c->originalProject];
                    originalsWeight[c->originalProject] += c->weight;
                    originalsWeightProjects[c->originalProject] += c->weightProjects;
                    originalsWeightRoots[c->originalProject] += c->weightRoots;
                }
                std::cerr << "Calculating project summaries..." << std::endl;
                std::ofstream cs(DataDir.value() + "/projectFolderCloneSummary.csv");
                cs << "#projectId,numCommits,numOriginals,weightOriginals,weightProjectsOriginals,weightRootsOriginats,numClones,numOwnClones, numUniqueClones, numUniqueOwnClones" << std::endl;
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    size_t numClones = 0;
                    std::unordered_set<Clone*> uniqueClones;
                    size_t numOwnClones = 0;
                    std::unordered_set<Clone*> uniqueOwnClones;
                    for (Commit * c : p->commits) {
                        assert (c != nullptr);
                        for (auto i : c->introducedClones) {
                            Clone * clone = i.second;
                            assert(clone != nullptr);
                            if (clone->originalProject == p) {
                                ++numOwnClones;
                                uniqueOwnClones.insert(clone);
                            } else {
                                ++numClones;
                                uniqueClones.insert(clone);
                            }
                        }
                    }
                    cs << p->id << "," << p->commits.size() << "," << numOriginals[p] << "," << originalsWeight[p] << "," << originalsWeightProjects[p] << "," << originalsWeightRoots[p] << "," << numClones << "," << numOwnClones << "," << uniqueClones.size() << "," << uniqueOwnClones.size() << std::endl;
                }
            }

            /** Goes for each project and analyzes its structure looking for changes into clones found therein. 
             
                For each clone occurence, identified by project, commit, path and clone id, we calculate the following:

                - number of changes to the files belonging to the clone, 
                - number of files affected by the changes 
                - number of deletions belonging to the clone
                
             */
            void cloneHistories() {
                std::cerr << "Analyzing project clone histories..." << std::endl;
                std::ofstream f(DataDir.value() + "/folderClonesHistorySummary.csv");
                f << "#projectId,commitId,path,cloneId,files,changedFiles,changes,deletedFiles,deletions,merges" << std::endl;
                size_t i =0;
                for (Project * p : projects_) {
                    if (p == nullptr)
                        continue;
                    std::cerr << (i++) << "    \r";
                    std::vector<CloneOccurence*> clones;
                    CommitForwardIterator<Commit, TrackedClonesMap> ci([&, this](Commit * c, TrackedClonesMap & tc ) {
                        // first track all deletions
                        for (auto i : c->changes)
                            if (i.second == FILE_DELETED)
                                tc.trackChange(i.first, i.second);
                        /* For each clone introduced by the commit, the clone occurence is added to the project map, and its files are marked as tracked.
                         */
                        std::vector<CloneOccurence*> cos;
                        for (auto i : c->introducedClones) {
                            CloneOccurence * co = new CloneOccurence(p, c, i.first, i.second);
                            clones.push_back(co);
                            cos.push_back(co);
                            // now for all changes, see if they belong to the clone and if so, add them to the tracked map
                            for (auto i : c->changes) {
                                if (i.second == FILE_DELETED)
                                    continue;
                                if (co->contains(paths_[i.first])) {
                                    tc.addPath(i.first, co);
                                    ++co->paths;
                                }
                            }
                        }
                        /* Now for each change in the commit, if the change goes to a tracked file, update the clone occurence statistics.
                         */
                        for (auto i : c->changes)
                            if (i.second != FILE_DELETED) {
                                tc.trackChange(i.first, i.second);
                            }
                        for (auto i : cos) {
                            if (i->changes != i->paths) {
                                std::cout << i->changes << " -- " << i->paths << std::endl;
                                std::cout << i->path << std::endl;
                                std::cout << i->clone->id << std::endl;
                                std::cout << i->project->id << std::endl;
                            }
                            assert(i->changes == i->paths);
                            i->changedPaths.clear();
                            i->changes = 0;
                        }
                        // always continue
                        return true;
                    });
                    for (Commit * c : p->commits)
                        if (c->numParents == 0)
                            ci.addInitialCommit(c);
                    ci.process();
                    /* And output the clone occurence information:
                     */
                    for (auto i : clones) {
                        //std::cout << i->clone->id << "," << i->paths << "," << i->clone->numFiles << std::endl;
                        assert(i->paths <= i->clone->numFiles);
                        assert(i->changedPaths.size() <= i->paths);
                        assert(i->deletedPaths.size() <= i->paths);
                        assert(i->changedPaths.size() <= i->changes + i->merges);
                        assert(i->deletedPaths.size() <= i->deletions);
                        f << i->project->id << "," <<
                            i->commit->id << "," <<
                            helpers::escapeQuotes(i->path) << "," <<
                            i->clone->id << "," <<
                            i->paths << "," <<
                            i->changedPaths.size() << "," <<
                            i->changes << "," <<
                            i->deletedPaths.size() << "," << 
                            i->deletions << "," <<
                            i->merges << std::endl;
                        delete i;
                    }
                }
            }

            /* All projects. */
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            std::vector<std::string> paths_;
            std::vector<Clone*> clones_;
        };


        


        
    } // anonymous namespace

    






    void FolderCloneHistoryAnalysis(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Analyzer a;
        a.initialize();
        a.originalsWeight();
        a.projectsSummary();
        a.cloneHistories();
        
    }

    
} // namespace dejavu
