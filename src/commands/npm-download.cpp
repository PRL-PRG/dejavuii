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

            std::string get_file(std::string const & hash, std::string const & path) {
                std::stringstream s;

                s << "https://raw.githubusercontent.com/"
                  << user << "/" << repo << "/"
                  << hash << "/"
                  << path;

                return s.str();
            }
        };

        void LoadNPMProjects(std::unordered_set<unsigned> &npm_project_ids,
                          std::unordered_set<NPMProject *> &npm_projects) {
            clock_t timer = clock();
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
            clock_t timer = clock();
            unsigned discarded = 0;

            std::string task = "extracting project IDs of interesting projects (numManualChanges > 0)";
            helpers::StartTask(task, timer);

            new NPMSummaryDetailLoader([&](unsigned projectId, 
                                           std::string const &path, 
                                           std::string const &name,
                                           unsigned numVersions, 
                                           unsigned numFiles, 
                                           unsigned numManualChanges,
                                           unsigned numManualChangesOriginal, 
                                           unsigned numDeletions,
                                           unsigned numCompleteDeletions, 
                                           unsigned numChangedFiles,
                                           unsigned numChangedFilesOriginal, 
                                           unsigned numDeletedFiles,
                                           unsigned numChangingCommits, 
                                           unsigned numchangingCommitsOriginal,
                                           unsigned numDeletingCommits, 
                                           unsigned numActiveFiles){
                if (numManualChanges > 0) {
                    npm_project_ids.insert(projectId);
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Loaded " << npm_project_ids.size() << " NPM projects" << std::endl;
            std::cerr << "Discarded " << discarded << " projects" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadPackageDotJSONIds(std::unordered_map<unsigned, std::string> &package_dot_json_ids) {

            clock_t timer = clock();
            unsigned discarded = 0;

            std::string task = "find IDs of paths that fit 'package.json'";
            helpers::StartTask(task, timer);
            std::string ending = "package.json";

            new PathLoader([&](unsigned id, std::string const & path) {
                if (path == ending) {
                    package_dot_json_ids[id] = path;
                } else if (ending.size() > path.size()) {
                    ++ discarded;
                } else if (std::equal(ending.rbegin(), ending.rend(), path.rbegin())) {
                    package_dot_json_ids[id] = path;
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Selected " << package_dot_json_ids.size() << " path IDs" << std::endl;
            std::cerr << "Discarded " << discarded << " path IDs" << std::endl;

            helpers::FinishTask(task, timer);
        }

        typedef struct {
            unsigned commitId;
            unsigned contentsId;
            unsigned pathId;
            std::string path;
        } CommitInfo;

        void LoadCommitsThatChangeToProjectDotJSON(std::unordered_set<unsigned> const &npm_project_ids,
                                                   std::unordered_map<unsigned, std::string> const &package_dot_json_ids,
                                                   std::unordered_map<unsigned, std::vector<CommitInfo>> &package_dot_json_commit_ids) {
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

                CommitInfo commitInfo;
                commitInfo.commitId = commitId;
                commitInfo.contentsId = contentsId;
                commitInfo.pathId = pathId;
                commitInfo.path = package_dot_json_ids.at(pathId);

                package_dot_json_commit_ids[projectId].push_back(commitInfo);
            });

            std::cerr << "Kept " << kept << " commit IDs" << std::endl;
            std::cerr << "Discarded " << discarded << " commit IDs" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadHashes(std::unordered_map<unsigned, std::string> &hashes) {
            clock_t timer = clock();
            std::string task = "loading (all) hashes";
            helpers::StartTask(task, timer);

            new HashToIdLoader([&](unsigned id, std::string const & hash){
                assert(hashes.find(id) == hashes.end());
                hashes[id] = hash;
            });

            helpers::FinishTask(task, timer);
        }

        class Download {
        public:
            Download(std::string url, std::string dir, std::string file):
                    url(url), dir(dir), file(file), path(dir + "/" + file) {}
            std::string url;
            std::string dir;
            std::string file;
            std::string path;

            std::string toCSV() {
                return url + "," + dir + "," + file;
            }
        };

        void PrepareForDownload(std::unordered_set<NPMProject *> const &npm_projects,
                                std::unordered_map<unsigned, std::string> const &hashes,
                                std::unordered_map<unsigned, std::vector<CommitInfo>> const &package_dot_json_commit_id,
                                std::vector<Download *> &downloads) {

            clock_t timer = clock();
            size_t inspected_projects = 0;
            size_t inspected_commits = 0;
            size_t skipped_projects = 0;
            size_t skipped_commits = 0;
            std::string task = "preparing list of files to download";
            helpers::StartTask(task, timer);

            for (NPMProject *project : npm_projects) {
                if (package_dot_json_commit_id.find(project->id)
                    == package_dot_json_commit_id.end()) {
                    ++skipped_projects;
                    continue;
                }
                ++inspected_projects;
                auto &commits = package_dot_json_commit_id.at(project->id);
                for (CommitInfo const &commit : commits) {

                    if (hashes.find(commit.commitId) == hashes.end()) {
                        std::cerr << std::endl
                                  << "Cannot find hash for commit " << commit.commitId
                                  << std::endl;
                        ++skipped_projects;
                        continue;
                    }

                    std::string commit_hash = hashes.at(commit.commitId);
                    std::string url = project->get_file(commit_hash, commit.path);
                    std::stringstream file;
                    file << commit.contentsId;

                    std::stringstream dir;
                    dir << "/package.json/" << project->id << "/"
                     // << commit.path << "/";
                        << commit.pathId << "/";

                    Download *download = new Download(url, dir.str(), file.str());
                    downloads.push_back(download);

                    helpers::Count(inspected_commits);
                }
            }

            std::cerr << "Projects (skipped):" << inspected_projects
                      << "(" << skipped_projects << ")" << std::endl;
            std::cerr << "Commits (skipped):" << inspected_commits
                      << "(" << skipped_commits << ")" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void SaveDownload(std::vector<Download *> const &downloads) {
            std::string filename = DataDir.value() + "/package.json/__all.csv";

            clock_t timer = clock();
            std::string task = "saving list of stuff to download";
            size_t items = 0;
            helpers::StartCounting(items);
            helpers::StartTask(task, timer);

            std::ofstream s(filename);
            if (! s.good()) {
                ERROR("Unable to open file " << filename << " for writing");
            }

            s << "url,filename" << std::endl;
            for (Download *download : downloads) {
                s << DataDir.value() << "/" << download->path << ","
                  << download->url << std::endl;
                helpers::Count(items);
            }

            s.close();

            helpers::FinishCounting(items);
            helpers::FinishTask(task, timer);
        }

        void DownloadAll(std::vector<Download *> const &downloads) {
            std::string filename = DataDir.value() + "/package.json/__failed.csv";

            clock_t timer = clock();
            std::string task = "download stuff";
            size_t downloaded = 0;
            size_t failed = 0;
            size_t attempted = 0;
            helpers::StartTask(task, timer);

            std::ofstream s(filename);
            if (! s.good()) {
                ERROR("Unable to open file " << filename << " for writing");
            }

            for (Download *download : downloads) {
                std::stringstream mkdir;
                mkdir << "mkdir -p "
                      << DataDir.value() << "/" << download->dir;

                std::stringstream wget;
                wget << "wget -nv "
                     << "-O " << DataDir.value() << "/" << download->path
                     << " " << download->url;

                int status = system(mkdir.str().c_str());
                assert(status == 0);

                status = system(wget.str().c_str());
                if (status != 0) {
                    s << download->toCSV() << std::endl;
                    ++failed;
                } else {
                    ++downloaded;
                }

                ++attempted;
                std::cerr << "Downloaded " << attempted << " out of "
                          << downloads.size() << std::endl;
            }

            s.close();

            helpers::FinishCounting(attempted);
            std::cerr << "Downloaded: " << downloaded << std::endl;
            std::cerr << "Failed to download: " << failed << std::endl;

            helpers::FinishTask(task, timer);
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

        std::unordered_map<unsigned, std::string> package_dot_json_ids;
        LoadPackageDotJSONIds(package_dot_json_ids);

        std::unordered_map<unsigned, std::vector<CommitInfo>> package_dot_json_commit_ids;
        LoadCommitsThatChangeToProjectDotJSON(npm_project_ids,
                                              package_dot_json_ids,
                                              package_dot_json_commit_ids);

        std::unordered_map<unsigned, std::string> hashes;
        LoadHashes(hashes);

        std::vector<Download *> downloads;
        PrepareForDownload(npm_projects,
                           hashes,
                           package_dot_json_commit_ids,
                           downloads);

        SaveDownload(downloads);

        DownloadAll(downloads);
    }
    
} // namespace dejavu
