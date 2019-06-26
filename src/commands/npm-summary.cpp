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
            unsigned npmFileChanges = 0;
            unsigned npmFileDeletions = 0;
            uint64_t firstTime = 0;
            uint64_t lastTime = 0;
            bool hasNPM = false;
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
            std::unordered_set<uint64_t> manualChangesOriginal;
            
            std::unordered_set<uint64_t> deletions;

            std::unordered_set<unsigned> completeDeletions;

            std::unordered_set<unsigned> files;

            std::unordered_set<unsigned> changedFiles;
            std::unordered_set<unsigned> changedFilesOriginal;
            std::unordered_set<unsigned> deletedFiles;
            std::unordered_set<unsigned> changingCommits;
            std::unordered_set<unsigned> changingCommitsOriginal;
            std::unordered_set<unsigned> deletingCommits;

            NPMPackage(std::string const & name, std::string const & root, unsigned packageJson):
                name(name),
                path(root),
                packageJson(packageJson) {
                // TODO determine name
                
            }

            void mergeWith(NPMPackage const & other) {
                assert(name == other.name);
                //assert(packageJson == other.packageJson);
                versions.insert(other.versions.begin(), other.versions.end());
                activeFiles.insert(other.activeFiles.begin(), other.activeFiles.end());
                manualChanges.insert(other.manualChanges.begin(), other.manualChanges.end());
                manualChangesOriginal.insert(other.manualChangesOriginal.begin(), other.manualChangesOriginal.end());
                deletions.insert(other.deletions.begin(), other.deletions.end());
                completeDeletions.insert(other.completeDeletions.begin(), other.completeDeletions.end());
                files.insert(other.files.begin(), other.files.end());
                changedFiles.insert(other.changedFiles.begin(), other.changedFiles.end());
                changedFilesOriginal.insert(other.changedFilesOriginal.begin(), other.changedFilesOriginal.end());
                deletedFiles.insert(other.deletedFiles.begin(), other.deletedFiles.end());
                changingCommits.insert(other.changingCommits.begin(), other.changingCommits.end());
                changingCommitsOriginal.insert(other.changingCommitsOriginal.begin(), other.changingCommitsOriginal.end());
                deletingCommits.insert(other.deletingCommits.begin(), other.deletingCommits.end());
            }

            // we have project, package root, package name, 
            friend std::ostream & operator << (std::ostream & s, NPMPackage const & p) {
                s << helpers::escapeQuotes(p.path) << ","
                  << helpers::escapeQuotes(p.name) << ","
                  << p.versions.size() << ","
                  << p.files.size() << ","
                  << p.manualChanges.size() << ","
                  << p.manualChangesOriginal.size() << ","
                  << p.deletions.size() << ","
                  << p.completeDeletions.size() << ","
                  << p.changedFiles.size() << ","
                  << p.changedFilesOriginal.size() << ","
                  << p.deletedFiles.size() << ","
                  << p.changingCommits.size() << ","
                  << p.changingCommitsOriginal.size() << ","
                  << p.deletingCommits.size() << ","
                  << p.activeFiles.size();
                return s;
            }

        };


        class State {
        public:
            State() {
            }

            State(const State & other) {
                mergeWith(other, nullptr);
            }

            void mergeWith(State const & other, Commit *c) {
                for (auto const & i : other.packages_) {
                    auto j = packages_.find(i.first);
                    if (j == packages_.end())
                        packages_.insert(i);
                    else
                        j->second.mergeWith(i.second);
                }
            }

            void handleFileDeletion(Commit * c, std::string const & root, unsigned pathId) {
                auto i = packages_.find(root);
                if (i == packages_.end())
                    std::cout << root << std::endl;
                assert(i != packages_.end());
                i->second.activeFiles.erase(pathId);
            }

            void handleManualFileDeletion(Commit * c, std::string const & root, unsigned pathId) {
                auto i = packages_.find(root);
                if (i == packages_.end())
                    std::cout << root << std::endl;
                assert(i != packages_.end());
                i->second.deletions.insert(Join2Unsigned(c->id, pathId));
                i->second.activeFiles.erase(pathId);
                i->second.deletedFiles.insert(pathId);
                i->second.deletingCommits.insert(c->id);
            }

            void addPackageVersion(std::string const & name, std::string const & root, unsigned packageJsonId, unsigned version) {
                auto i = packages_.find(root);
                if (i == packages_.end())
                    i = packages_.insert(std::make_pair(root, NPMPackage(name, root, packageJsonId))).first;
                i->second.versions.insert(version);
            }

            void registerFile(std::string const & packageRoot, unsigned fileId) {
                auto i = packages_.find(packageRoot);
                assert(i != packages_.end());
                i->second.activeFiles.insert(fileId);
                i->second.files.insert(fileId);
            }

            void registerFileChange(Commit * c, std::string const & packageName, std::string const & packageRoot, unsigned fileId, bool original) {
                auto i = packages_.find(packageRoot);
                if (i == packages_.end()) {
                    // packageJson id will be 0 for packages created this way
                    i = packages_.insert(std::make_pair(packageRoot, NPMPackage(packageName, packageRoot, 0))).first;
                }
                i->second.activeFiles.insert(fileId);
                i->second.files.insert(fileId);
                // TODO maybe I want this exclusive?
                i->second.manualChanges.insert(Join2Unsigned(c->id, fileId));
                i->second.changedFiles.insert(fileId);
                i->second.changingCommits.insert(c->id);
                if (original) {
                    i->second.manualChangesOriginal.insert(Join2Unsigned(c->id, fileId));
                    i->second.changedFilesOriginal.insert(fileId);
                    i->second.changingCommitsOriginal.insert(c->id);
                } 
            }

            void mergeEmptyPackages(Commit * c, std::unordered_map<std::string, NPMPackage> & into) {
                for(auto i = packages_.begin(), e = packages_.end(); i != e; ) {
                    if (i->second.activeFiles.empty()) {
                        i->second.completeDeletions.insert(c->id);
                        auto j = into.find(i->first);
                        if (j == into.end())
                            into.insert(*i);
                        else
                            j->second.mergeWith(i->second);
                        i = packages_.erase(i);
                    } else {
                        ++i;
                    }
                }
            }

            void mergeAllPackages(Commit * c, std::unordered_map<std::string, NPMPackage> & into) {
                for(auto i = packages_.begin(), e = packages_.end(); i != e; ++i) {
                    auto j = into.find(i->first);
                    if (j == into.end())
                        into.insert(*i);
                    else
                        j->second.mergeWith(i->second);
                }
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
                            // break the path into segments
                            // we are interested in node_modules/folder/...
                            std::vector<std::string> ps = helpers::Split(path, '/');
                            ++totalPaths;
                            size_t i = ps.size() - 2;
                            while (i < ps.size()) {
                                if (ps[i] == "node_modules") {
                                    std::string name = ps[i + 1];
                                    std::string root = ps[0];
                                    for (size_t j = 1; j <= i + 1; ++j)
                                        root = root + "/" + ps[j];
                                    packageRoots_.insert(std::make_pair(id, PathInfo(name, root, i + 3 == ps.size() && ps.back() == "package.json")));
                                    ++npmPaths;
                                    break;
                                }
                                --i;
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
                    std::cerr << "Loading commit parents ... " << std::endl;
                    CommitParentsLoader{[this](unsigned id, unsigned parentId){
                            Commit * c = commits_[id];
                            Commit * p = commits_[parentId];
                            assert(c != nullptr);
                            assert(p != nullptr);
                            c->addParent(p);
                        }};
                    
                }
                {
                    std::cerr << "Loading file changes ... " << std::endl;
                    size_t totalChanges = 0;
                    std::unordered_map<unsigned, unsigned> contents;
                    FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++totalChanges;
                            Project * p = projects_[projectId];
                            Commit * c = commits_[commitId];
                            assert(p != nullptr);
                            assert(c != nullptr);
                            p->addCommit(c);
                            c->addChange(pathId, contentsId);
                            auto i = contents.find(contentsId);
                            if (i == contents.end())
                                contents.insert(std::make_pair(contentsId, 0));
                            else
                                ++i->second;
                        }};
                    std::cerr << "    " << totalChanges << " total changes read" << std::endl;
                    for (auto i : contents)
                        if (i.second == 0)
                            originalContents_.insert(i.first);
                    std::cerr << "    " << originalContents_.size() << " unique files" << std::endl;
                }
            }

            void analyzeProjects() {
                std::cerr << "Calculating project summaries..." << std::endl;
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
                    if (!p->npmPaths.empty())
                        p->hasNPM = true;
                    p->paths.clear();
                    p->npmPaths.clear();
                }
                
            }
            void analyzeDetails() {
                std::cerr << "Calculating project NPM details " << std::endl;
                packageDetails_.open(DataDir.value() + "/npm-summary-details.csv");
                packageDetails_ << "projectId,path,name,numVersions,numFiles,numManualChanges,numManualChangesOriginal,numDeletions,numCompleteDeletions,numChangedFiles,numChangedFilesOriginal,numDeletedFiles,numChangingCommits,numChangingCommitsOriginal,numDeletingCommits,numActiveFiles" << std::endl;
                unsigned i = 0;
                for (Project * p : projects_) {
                    if (++i % 1000 == 0)
                        std::cerr << " : " << i << '\r' << std::flush;
                    if (p == nullptr || p->hasNPM == false)
                        continue;
                    analyzeProjectProper(p);
                    p->paths.clear();
                    p->npmPaths.clear();
                }
                
            }

        private:
            class PathInfo {
            public:
                std::string name;
                std::string root;
                bool isJson;

                PathInfo(std::string const & name, std::string const & root, bool isJson):
                    name(name),
                    root(root),
                    isJson(isJson) {
                }

                bool valid() const {
                    return !name.empty();
                }
            };


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
                        // check if any package has been completely deleted and if so, mark it as such and update the main package
                        state.mergeEmptyPackages(c, packages);
                        // now deal with package.jsons since any changes to them mean that the package has version update and therefore we ignore changes to it
                        std::unordered_set<std::string> changedPackages;
                        for (auto i : c->changes) {
                            PathInfo const & pi = getNPMPathInfo(i.first);
                            if (pi.isJson) { // it has to be valid if it is package.json
                                state.addPackageVersion(pi.name, pi.root, i.first, i.second);
                                changedPackages.insert(pi.root);
                            }
                        }
                        // deal with deletions now, file deletion is manual if package.json is not changed. Issue is if package.json is deleted, and so are other files. If all other files are deleted then the package will not exist and will be uninteresting even if we mark the deletions as manual. If some files survive and package.json is deleted, then it is indeed manual deletion, so it is correct. 
                        for (auto i : c->deletions) {
                            PathInfo const & pi = getNPMPathInfo(i);
                            if (pi.valid()) {
                                if (changedPackages.find(pi.root) != changedPackages.end())
                                    state.handleFileDeletion(c, pi.root, i);
                                else
                                    state.handleManualFileDeletion(c, pi.root, i);
                            }
                        }
                        // now deal with other changes, for each change determine if it belongs to a package and if it does, update the package
                        for (auto i : c->changes) {
                            PathInfo const & pi = getNPMPathInfo(i.first);
                            if (pi.valid()) {
                                if (changedPackages.find(pi.root) != changedPackages.end())
                                    state.registerFile(pi.root, i.first);
                                else
                                    state.registerFileChange(c, pi.name, pi.root, i.first, isOriginal(i.second));
                            }
                        }

                        if (c->children.empty())
                            state.mergeAllPackages(c, packages);
                    return true;
                });
                cfi.process();
                /** Output the packages info
                 */
                for (auto i : packages) {
                    packageDetails_ << p->id << "," << i.second << std::endl;
                }
            }

            /** Returns true if the given contents id was seen only once in the entire corpus.

                TODO maybe we want unique files *and* the originals? 
             */
            bool isOriginal(unsigned contentsId) {
                return originalContents_.find(contentsId) != originalContents_.end();
            }

            /** Returns true if given path is NPM file.
             */
            bool isNPMPath(unsigned pathId) {
                return packageRoots_.find(pathId) != packageRoots_.end();
            }

            PathInfo const & getNPMPathInfo(unsigned path) {
                auto i = packageRoots_.find(path);
                if (i == packageRoots_.end())
                    return notNPM_;
                else
                    return i->second;
            }



            PathInfo notNPM_ = PathInfo("","",false);
            
            /** path -> isNPM?
             */
            std::unordered_map<unsigned, PathInfo> packageRoots_;
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;
            std::ofstream packageDetails_;
            std::unordered_set<unsigned> originalContents_;

            
        }; 

        
    } // anonymous namespace

    void NPMSummary(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Summary s;
        s.loadData();
        s.analyzeProjects();
        s.analyzeDetails();
        
        
    }
    
} // namespace dejavu
