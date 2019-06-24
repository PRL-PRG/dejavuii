#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>

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

            std::unordered_set<unsigned> activeFiles;

            std::unordered_set<uint64_t> manualChanges;
            
            std::unordered_set<uint64_t> deletions;


            NPMPackage(std::string const & root, unsigned packageJson):
                path(root),
                packageJson(packageJson) {
                // TODO determine name
                
            }

            void mergeWith(NPMPackage const & other) {
                assert(name == other.name);
                assert(packageJson == other.packageJson);
                versions.insert(other.versions.begin(), other.versions.end());
                activeFiles.insert(other.activeFiles.begin(), other.activeFiles.end());
                manualChanges.insert(other.manualChanges.begin(), other.manualChanges.end());
                deletions.insert(other.deletions.begin(), other.deletions.end());
            }

        };


        class State {
        public:
            State() {
            }

            State(const State & other) {
                
            }

            void mergeWith(State const & other, Commit *c) {
                for (auto i : other.packages_) {
                    auto j = packages_.find(i.first);
                    if (j == packages_.end())
                        packages_.insert(i);
                    else
                        j->second.mergeWith(i.second);
                }
            }

            void handleFileDeletion(Commit * c, unsigned pathId, std::string const & path) {
                for (auto i : packages_)
                    if (path.find(i.first) == 0) {
                        i.second.deletions.insert(Join2Unsigned(c->id, pathId));
                        i.second.activeFiles.erase(pathId);
                    }
            }

            void addPackageVersion(std::string const & root, unsigned packageJsonId, unsigned version) {
                auto i = packages_.find(root);
                if (i == packages_.end())
                    i = packages_.insert(std::make_pair(root, NPMPackage(root, packageJsonId))).first;
                i->second.versions.insert(version);
            }

        private:
            std::unordered_map<std::string, NPMPackage> packages_;
            
        };
        

        class Summary {
        public:
            void loadData() {
                {
                    std::cerr << "Loading paths..." << std::endl;
                    size_t totalPaths = 0;
                    size_t npmPaths = 0;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            /*
                            ++totalPaths;
                            if (id >= paths_.size())
                                paths_.resize(id + 1);
                            if (IsNPMPath(path)) {
                                ++npmPaths;
                                paths_[id] = true;
                            } else {
                                paths_[id] = false;
                                }*/
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
                    analyzeProjectProper(p);
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
                        if (isNPMPath(i))
                            ++p->npmFileDeletions;
                    }
                    for (auto i : c->changes) {
                        if (isNPMPath(i.first)) {
                            ++p->npmFileChanges;
                            p->npmPaths.insert(i.first);
                        }
                        p->paths.insert(i.first);
                    }
                }
                p->npmFileChanges -= p->npmPaths.size();
            }

            /** Proper analysis of the project, which calculates all paths, all node modules and so on.


                don't use dummy state, but keep active NPMPackages

             */
            void analyzeProjectProper(Project * p) {
                std::unordered_map<std::string, NPMPackage> packages;
                CommitForwardIterator<Project, Commit, State> cfi(p, [&,this](Commit * c, State & state) {
                        // first deal with deletions
                        for (auto i : c->deletions) 
                            state.handleFileDeletion(c, i, pathStrings_[i]);
                        // check if any package has been completely deleted and if so, mark it as such and update the main package
                        // TODO
                        // now deal with package.jsons since any changes to them mean that the package has version update and therefore we ignore changes to it
                        std::unordered_set<std::string> changedPackages;
                        for (auto i : c->changes) {
                            if (isPackageJson(i.first)) {
                                std::string const &  root = getPackageRoot(i.first);
                                state.addPackageVersion(root, i.first, i.second);
                                changedPackages.insert(root);
                            }
                        }
                        // now deal with other changes, for each change determine if it belongs to a package and if it does, update the package
                        for (auto i : c->changes) {
                            
                        }

                        /*




                        
                        // second check any package.json updates because these are package versions actually
                        std::unordered_set<std::string> updatedPackages;
                        for (auto i : c->changes) {
                            if (isNPMPackageJson(i.first)) {
                                std::string root = packageRoot(i.first);
                                updatedPackages.insert(root);
                                auto i = packages.find(root);
                                if (i == packages.end())
                                    i = packages.insert(std::make_pair(root, new NPMPackage(root, i.first))).first;
                                i->second->versions.insert(i.second);
                                                        
                        }
                        // and finally, deal with the

                        */

                    return true;
                });
                
            }


            /** Returns true if given path is NPM file.
             */
            bool isNPMPath(unsigned pathId) {
                return packageRoots_.find(pathId) != packageRoots_.end();
            }

            /** Returns true if the given path is package.json of a NPM package.
             */
            bool isPackageJson(unsigned path) {
                return false;
            }

            /** Returns the root folder of the package in the path in question.

                Since this is a time consuming operation, it makes sense to pre-cache this
             */
            
            std::string const & getPackageRoot(unsigned path) {
            }

            
            /** path -> isNPM?
             */
            std::vector<std::string> pathStrings_;
            std::unordered_map<unsigned, std::string> packageRoots_;
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
