
#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {

        class ProjectInfo {
        public:
            std::unordered_set<unsigned> paths;
            std::unordered_set<unsigned> npmPaths;
            size_t changes;
            size_t npmChanges;
            size_t deletes;
            size_t npmDeletes;

            ProjectInfo():
                changes(0),
                npmChanges(0),
                deletes(0),
                npmDeletes(0) {
            }

            void addChange(unsigned pathId, unsigned contentsId, std::unordered_set<unsigned> const & npmPaths) {
                if (npmPaths.find(pathId) == npmPaths.end()) {
                    // not an NPM path
                    if (contentsId == FILE_DELETED)
                        ++deletes;
                    else
                        ++changes;
                    paths.insert(pathId);
                } else {
                    // NPM path
                    if (contentsId == FILE_DELETED)
                        ++npmDeletes;
                    else
                        ++npmChanges;
                    this->npmPaths.insert(pathId);
                }
            }

        };

        class NPMCounter {
        public:
            void loadValidPaths() {
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[this](unsigned id, std::string const & path){
                        if (path.find("node_modules/") == 0 || path.find("/node_modules/") != std::string::npos) 
                            paths_.insert(id);
                    }};
                std::cerr << "    " << paths_.size() << " paths loaded" << std::endl;
            }

            void countChanges() {
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        projects_[projectId].addChange(pathId, contentsId, paths_);
                    }};
                // and now output the results
                std::cerr << "Writing project summaries..." << std::endl;
                std::ofstream f(DataDir.value() + "/npmModulesCounts.csv");
                f << "#projectId,paths,changes,deletions,npmPaths,npmChanges,npmDeletions" << std::endl;
                for (auto & i : projects_) {
                    ProjectInfo const & p = i.second;
                    f << i.first << "," << p.paths.size() << "," << p.changes << "," << p.deletes << "," << p.npmPaths.size() << "," << p.npmChanges << "," << p.npmDeletes << std::endl;
                }
        }

        private:
            std::unordered_set<unsigned> paths_;

            std::unordered_map<unsigned, ProjectInfo> projects_;



        }; //NPMCounter


    } // anonymous namespace

    void NPMModuleCounts(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        NPMCounter c;
        c.loadValidPaths();
        c.countChanges();

    }

} //namespace dejavu