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
                id(0),
                time(0),
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

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;

            /** Number of unique clones whose original is in the project. 
             */
            size_t numOriginals;

            std::unordered_set<Commit *> commits;

            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt),
                numOriginals(0) {
            }

            void addCommit(Commit * c) {
                commits.insert(c);
            }
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

            Clone(unsigned id, unsigned numFiles, Project * project, Commit * commit, std::string const & rootDir):
                id(id),
                numFiles(numFiles),
                originalProject(project),
                originalCommit(commit),
                originalRoot(rootDir) {
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
                        paths_[id] = path;
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
                        ++project->numOriginals;
                        Commit * commit = commits_[commitId];
                        clones_[id] = new Clone(id, numFiles, project, commit, rootDir);
                    }};
                std::cerr << "    " << clones_.size() << " unique clones loaded" << std::endl;
                std::cerr << "Loading clones..." << std::endl;
                size_t missingClones = 0;
                FolderClonesLoader{[this, &missingClones](unsigned projectId, unsigned commitId, std::string const & rootDir, unsigned cloneId){
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
                std::cerr << "    " << missingClones << " missing clones";
            }

            /** Generates a summary table where for each project the number of clones and the number of clone originals is reported.
             */
            void projectsSummary() {
                std::cerr << "Calculating project summaries..." << std::endl;
                std::ofstream cs(DataDir.value() + "/projectFolderCloneSummary.csv");
                cs << "#projectId,numCommits,numOriginals,numClones,numOwnClones, numUniqueClones, numUniqueOwnClones" << std::endl;
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
                    cs << p->id << "," << p->commits.size() << "," << p->numOriginals << "," << numClones << "," << numOwnClones << "," << uniqueClones.size() << "," << uniqueOwnClones.size() << std::endl;
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
        a.projectsSummary();
        
    }

    
} // namespace dejavu
