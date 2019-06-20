#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

// - project, # commits, # paths, # npmPaths, # of updates to npm files, date of first and last commit dates


namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }

            std::unordered_set<unsigned> paths;
            std::unordered_set<unsigned> npmPaths;
            unsigned npmFileChanges;
            unsigned npmFileDeletions;
            uint64_t firstTime;
            uint64_t lastTime;
        };

        class NPMPackage {
        public:
            /** Name of the package.
             */
            std::string name;

            /** Path of the package (i.e. path of the folder where the package.json file resides).
             */
            std::string path;

            /** Path of the packageJson.
             */
            unsigned packageJson;
            
            /** Versions of the package represented as content ids of the package.json file.
              */
            std::unordered_set<unsigned> versions;

            /** I.e. number of changes of the project, that is changes to the package.json file.
              */
            unsigned versionChanges;

            /** Number of changes to files of the package fro commits that do not change package.json.
             */
            unsigned manualChanges;

            /** Manually changed unique files.
             */
            std::unordered_set<unsigned> manuallyChangedFiles;
            
            /** Commits that perform manual changes.
             */
            std::unordered_set<unsigned> manuallChangeCommits;
            
        };

        typedef BaseDummyState<Commit> DummyState;


        class Summary {
        public:
            void loadData() {
                {
                    std::cerr << "Loading paths..." << std::endl;
                    size_t totalPaths = 0;
                    size_t npmPaths = 0;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            ++totalPaths;
                            if (id >= paths_.size())
                                paths_.resize(id + 1);
                            if (IsNPMPath(path)) {
                                ++npmPaths;
                                paths_[id] = true;
                            } else {
                                paths_[id] = false;
                            }
                        }};
                    std::cerr << "    " << totalPaths << " total paths read" << std::endl;
                    std::cerr << "    " << npmPaths << " retained paths" << std::endl;
                }
                {
                    size_t totalProjects = 0;
                    std::cerr << "Loading projects ... " << std::endl;
                    ProjectLoader{DataDir.value() + "/projects.csv", [&,this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                            ++totalProjects;
                            if (id >= projects_.size())
                                projects_.resize(id + 1);
                            projects_[id] = new Project(id,createdAt);
                        }};
                    std::cerr << "    " << totalProjects << " total projects read" << std::endl;
                }
                {
                    std::cerr << "Loading commits ... " << std::endl;
                    size_t totalCommits = 0;
                    CommitLoader{[&,this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                            ++totalCommits;
                            if (id >= commits_.size())
                                commits_.resize(id + 1);
                            commits_[id] = new Commit(id, authorTime);
                        }};
                    std::cerr << "    " << totalCommits << " total commits read" << std::endl;
                }
                {
                    std::cerr << "Loading file changes ... " << std::endl;
                    size_t totalChanges = 0;
                    FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++totalChanges;
                            Project * p = projects_[projectId];
                            Commit * c = commits_[commitId];
                            assert(p != nullptr);
                            assert(c != nullptr);
                            p->addCommit(c);
                            c->addChange(pathId, contentsId);
                        }};
                    std::cerr << "    " << totalChanges << " total changes read" << std::endl;
                }
            }

            void analyzeProjects() {
                std::ofstream f(DataDir.value() + "/npm-summary.csv");
                f << "projectId,commits,firstTime,lastTime,numPaths,numNPMPaths,npmChanges,npmDeletions" << std::endl;
                unsigned i = 0;
                for (Project * p : projects_) {
                    if (++i % 1000 == 0)
                        std::cerr << " : " << i << '\r' << std::flush;
                    if (p == nullptr)
                        continue;
                    analyzeProject(p);
                    f << p->id << "," << p->commits.size() << "," << p->firstTime << "," << p->lastTime << "," << p->paths.size() << "," << p->npmPaths.size() << "," << p->npmFileChanges << "," << p->npmFileDeletions << std::endl;
                    p->paths.clear();
                    p->npmPaths.clear();
                }
                
            }

        private:


            void analyzeProject(Project * p) {
                p->npmFileChanges = 0;
                p->npmFileDeletions = 0;
                if (p->commits.size() == 0) {
                    return;
                }
                p->firstTime = (*p->commits.begin())->time;
                p->lastTime = (*p->commits.begin())->time;
                for (Commit * c : p->commits) {
                    if (c->time < p->firstTime)
                        p->firstTime = c->time;
                    if (c->time > p->lastTime)
                        p->lastTime = c->time;
                    for (auto i : c->deletions) {
                        if (paths_[i])
                            ++p->npmFileDeletions;
                    }
                    for (auto i : c->changes) {
                        if (paths_[i.first]) {
                            ++p->npmFileChanges;
                            p->npmPaths.insert(i.first);
                        }
                        p->paths.insert(i.first);
                    }
                }
                p->npmFileChanges -= p->npmPaths.size();
            }

            /** Proper analysis of the project, which calculates all paths, all node modules and so on.

             */
            void analyzeProjectProper(Project * p) {
                CommitForwardIterator<Project, Commit, DummyState> cfi(p, [&,this](Commit * c, DummyState &) {
                        // first deal with deletions

                        // second deal with changes
                        for (auto i : c->changes) {
                            if (isNPMPackageJson(i.first)) {
                                
                            }
                            
                        }

                    return true;
                });
                
            }



            /** Returns true if the given path is package.json of a NPM package.
             */
            bool isNPMPackageJson(unsigned path) {
                return false; 
            }


            
            /** path -> isNPM?
             */
            std::vector<bool> paths_;
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;

            
        }; 

        
    } // anonymous namespace

    


    void NPMSummary(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Summary s;
        s.loadData();
        //s.analyzeProjects();
        
    }
    
} // namespace dejavu
