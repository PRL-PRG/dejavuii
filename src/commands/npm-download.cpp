#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {

        class NPMProject {
        public:
            unsigned id;
            std::string const user;
            std::string const repo;

            NPMProject(unsigned id,
                       std::string const & user,
                       std::string const & repo):
                    id(id), user(user), repo(repo) {
            }

            std::string get_package_json() {
                std::stringstream s;

                s << "https://raw.githubusercontent.com/"
                << user << "/" << repo << "/master/package.json";

                return s.str();
            }

            std::string get_package_json(std::string const & hash) {
                std::stringstream s;

                s << "https://raw.githubusercontent.com/"
                  << user << "/" << repo << "/"
                  << hash << "/package.json";

                return s.str();
            }
        };

        void LoadNPMProjects(std::unordered_set<unsigned> &npm_project_ids,
                          std::unordered_set<NPMProject *> &npm_projects) {
            clock_t timer;
            unsigned discarded = 0;

            std::string task = "extracting project information (NPM projects only)";
            helpers::StartTask(task, timer);

            std::unordered_set<unsigned> insertedProjects;
            new ProjectLoader([&](unsigned id, std::string const & user,
                                  std::string const & repo, uint64_t createdAt){
                if (npm_project_ids.find(id) == npm_project_ids.end()) {
                    ++discarded;
                    return;
                }

                npm_projects.insert(new NPMProject(id, user, repo));
            });

            std::cerr << "Loaded " << npm_projects.size() << " NPM projects" << std::endl;
            std::cerr << "Discarded " << discarded << " projects" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadInterestingNPMProjectIDs(std::unordered_set<unsigned> &npm_project_ids) {
            clock_t timer;
            unsigned discarded = 0;

            std::string task = "extracting project IDs of interesting projects (numNPMChanges > 0)";
            helpers::StartTask(task, timer);

            new NPMSummaryLoader([&](unsigned projectId, unsigned commits, unsigned firstTime, unsigned lastTime, unsigned numPaths, unsigned numNPMPaths, unsigned npmChanges, unsigned npmDeletions){
                if (npmChanges > 0) {
                    npm_project_ids.insert(projectId);
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Loaded " << npm_project_ids.size() << " NPM projects" << std::endl;
            std::cerr << "Discarded " << discarded << " projects" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadPackageDotJSONIds(
                std::unordered_set<unsigned> &package_dot_json_ids) {
            clock_t timer;
            unsigned discarded = 0;

            std::string task = "find IDs of paths that fit 'project.json'";
            helpers::StartTask(task, timer);

            new PathLoader([&](unsigned id, std::string const & path){
                if (path == "package.json") {
                    package_dot_json_ids.insert(id);
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Selected " << package_dot_json_ids.size() << " path IDs" << std::endl;
            std::cerr << "Discarded " << discarded << " path IDs" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadCommitsThatChangeToProjectDotJSON(std::unordered_set<unsigned> const &npm_project_ids,
                                                   std::unordered_set<unsigned> const &package_dot_json_ids,
                                                   std::unordered_map<unsigned, std::unordered_set<unsigned>> &package_dot_json_commit_ids) {
            clock_t timer;
            unsigned discarded = 0;
            unsigned kept = 0;

            std::string task = "loading file changes relevant for interesting NPM projects and their project.json files";
            helpers::StartTask(task, timer);

            new FileChangeLoader([&](unsigned projectId, unsigned commitId,
                                     unsigned pathId, unsigned contentsId){

                if (package_dot_json_ids.find(pathId) == package_dot_json_ids.end()) {
                    ++discarded;
                    return;
                }

                if (npm_project_ids.find(projectId) == npm_project_ids.end()) {
                    ++discarded;
                    return;
                }

                ++kept;
                package_dot_json_commit_ids[projectId].insert(commitId);
            });

            std::cerr << "Kept " << kept << " commit IDs" << std::endl;
            std::cerr << "Discarded " << discarded << " commit IDs" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadHashes(std::unordered_map<unsigned, std::string> &hashes) {
            clock_t timer;
            std::string task = "loading (all) hashes";
            helpers::StartTask(task, timer);

            new HashToIdLoader([&](unsigned id, std::string const & hash){
                assert(hashes.find(id) == hashes.end());
                hashes[id] = hash;
            });

            helpers::FinishTask(task, timer);
        }

        void Download(std::unordered_set<NPMProject *> const &npm_projects,
                      std::unordered_map<unsigned, std::string> const &hashes) {
            for (NPMProject *project : npm_projects) {
                std::string commit_hash = hashes.at(project->id);
                std::string url = project->get_package_json(commit_hash);

                std::cerr << url << std::endl;; //FIXME actually download
            }
        }
    };

    void NPMDownload(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_set<unsigned> npm_project_ids;
        LoadInterestingNPMProjectIDs(npm_project_ids);

        std::unordered_set<NPMProject *> npm_projects;
        LoadNPMProjects(npm_project_ids, npm_projects);

        std::unordered_set<unsigned> package_dot_json_ids;
        LoadPackageDotJSONIds(package_dot_json_ids);

        std::unordered_map<unsigned, std::unordered_set<unsigned>> package_dot_json_commit_ids;
        LoadCommitsThatChangeToProjectDotJSON(npm_project_ids,
                                              package_dot_json_ids,
                                              package_dot_json_commit_ids);

        std::unordered_map<unsigned, std::string> hashes;
        LoadHashes(hashes);

        Download(npm_projects, hashes);
    }
    
} // namespace dejavu
