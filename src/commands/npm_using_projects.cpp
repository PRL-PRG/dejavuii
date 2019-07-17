#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {


    class Project {
    public:
        unsigned id;
        std::unordered_map<unsigned, unsigned> changingCommits;
        Project(unsigned id):
            id(id) {
        }
    }; 
    

    /** Finds all projects that use NPM,

        i.e. which contain package.json files in the root folder. This metric is not 100% correct, but is close enough (technically, a project may contain the package.json in a subfolder or so).
     */
    class PackageJsonFinder {
    public:
        void loadData() {
            {
                std::cerr << "Checking package.json path id..." << std::endl;
                std::ofstream f(OutputDir.value() + "/paths.csv");
                try {
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        if (path == "package.json")
                            throw id;
                    }};
                } catch (unsigned pathId) {
                    packageJsonPathId_ = pathId;
                    std::cerr << "    package.json: " << packageJsonPathId_ << std::endl;
                }
            }
            {
                std::cerr << "Analyzing projects for package.json changes..." << std::endl;
                size_t numProjects = 0;
                size_t numChanges = 0;
                FileChangeLoader{[&, this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        // not interested in deletions, or in non-package.json changes
                        if (pathId != packageJsonPathId_ || contentsId == FILE_DELETED)
                            return;
                        auto i = projects_.find(projectId);
                        if (i == projects_.end()) {
                            i = projects_.insert(std::make_pair(projectId, new Project(projectId))).first;
                            ++numProjects;
                        }
                        i->second->changingCommits.insert(std::make_pair(commitId, contentsId));
                        ++numChanges;
                    }};
                std::cerr << "    " << numProjects << " projects using package.json found" << std::endl;
                std::cerr << "    " << numChanges << " changes to package.json observed" << std::endl;
            }
        }

        /** Outputs the calculated information.
         */
        void output() {
            {
                std::cerr << "Writing projects using NPM..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectsUsingNPM.csv");
                f << "projectId" << std::endl;
                for (auto i : projects_)
                    f << i.first << std::endl;
            }
            {
                std::cerr << "Writing package.json file changes..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectPackageJsons.csv");
                f << "projectId,commitId,pathId,contentsId" << std::endl;
                for (auto i : projects_)
                    for (auto ch : i.second->changingCommits)
                        f << i.first << "," << ch.first << "," << packageJsonPathId_ << "," << ch.second << std::endl;
            }
        }

    private:
        unsigned packageJsonPathId_;
        std::unordered_map<unsigned, Project *> projects_;


        
    };


    
    void NPMUsingProjects(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        PackageJsonFinder pf;
        pf.loadData();
        pf.output();
        
    }

    
} // namespace dejavu
