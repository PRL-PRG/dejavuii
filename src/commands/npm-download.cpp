#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>
#include <sys/stat.h>
#include <fcntl.h>

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
        };

        struct CommitInfo {
            unsigned projectId;
            unsigned commitId;
            unsigned contentsId;
            unsigned pathId;
            std::string path;
        };

        struct Download {
            Download(std::string url, std::string dir, std::string file):
                    url(url), dir(dir), file(file), path(dir + "/" + file) {}
            std::string url;
            std::string dir;
            std::string file;
            std::string path;
        };

        void LoadInfoForInterestingProjects(std::unordered_set<unsigned> const &interesting_projects,
                                            std::unordered_map<unsigned, NPMProject *> &interesting_projects_info) {
            clock_t timer = clock();
            unsigned discarded = 0;

            std::string task = "extracting project information (NPM projects only)";
            helpers::StartTask(task, timer);

            std::unordered_set<unsigned> insertedProjects;
            new ProjectLoader([&](unsigned id, std::string const & user,
                                  std::string const & repo, uint64_t createdAt){
                if (interesting_projects.find(id) == interesting_projects.end()) {
                    ++discarded;
                    return;
                }

                interesting_projects_info[id] = new NPMProject(id, user, repo);
            });

            std::cerr << "Loaded " << interesting_projects_info.size() << " NPM projects" << std::endl;
            std::cerr << "Discarded " << discarded << " projects" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadInterestingProjectsAndPaths(std::unordered_set<unsigned> &interesting_projects,
                                             std::unordered_set<std::string> &paths_to_interesting_package_json,
                                             std::unordered_map<unsigned, std::unordered_set<std::string>> &interesting_projects_and_paths_to_package_json) {
            clock_t timer = clock();

            size_t discarded = 0;
            size_t added = 0;

            std::string task = "extracting interesting projects and paths to modified package.json within them (numManualChanges > 0)";
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
                                           unsigned numChangingCommitsOriginal,
                                           unsigned numDeletingCommits, 
                                           unsigned numActiveFiles){
                
                if (numManualChanges > 0) {
                    interesting_projects.insert(projectId);
                    paths_to_interesting_package_json.insert(path);
                    interesting_projects_and_paths_to_package_json[projectId].insert(path);
                    ++added;
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Loaded " << added << " NPM projects" << std::endl;
            std::cerr << "Discarded " << discarded << " projects" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadIDsOfInterestingPaths(std::unordered_set<std::string> const &paths_to_interesting_package_json,
                                       std::unordered_map<unsigned, std::string> &ids_of_paths_to_interesting_package_json) {

            clock_t timer = clock();
            unsigned discarded = 0;

            std::string task = "finding IDs of interesting paths to 'package.json' files";
            helpers::StartTask(task, timer);
            std::string ending = "package.json";

            new PathLoader([&](unsigned id, std::string const & path) {
                if (paths_to_interesting_package_json.find(path) != paths_to_interesting_package_json.end()) {
                    ids_of_paths_to_interesting_package_json[id] = path;
                } else {
                    ++discarded;
                }
            });

            std::cerr << "Selected " << ids_of_paths_to_interesting_package_json.size() << " path IDs" << std::endl;
            std::cerr << "Discarded " << discarded << " path IDs" << std::endl;

            helpers::FinishTask(task, timer);
        }

//        void SavePackageDotJSONIds(std::unordered_map<unsigned, std::string> const &package_dot_json_ids) {
//            clock_t timer = clock();
//            unsigned discarded = 0;
//
//            std::string task = "saving IDs of paths that fit 'package.json'";
//            helpers::StartTask(task, timer);
//
//            for (auto &it : package_dot_json_ids) {
//
//            }
//
//            helpers::FinishTask(task, timer);
//        }

          void LoadCommitsThatChangeProjectJSONPathsInInterestingProjects(
                  std::unordered_set<unsigned> const &interesting_projects,
                  std::unordered_map<unsigned, std::string> const &ids_of_paths_to_interesting_package_json,
                  std::vector<CommitInfo> &interesting_commits) {

            clock_t timer;
            size_t discarded = 0;
            size_t kept = 0;

            std::string task = "loading file changes for interesting projects that change interesting 'project.json' files";
            helpers::StartTask(task, timer);

            new FileChangeLoader([&](unsigned projectId, unsigned commitId,
                                     unsigned pathId, unsigned contentsId){

                if (ids_of_paths_to_interesting_package_json.find(pathId) ==
                        ids_of_paths_to_interesting_package_json.end()) {
                    ++discarded;
                    return;
                }

                if (interesting_projects.find(projectId) ==
                        interesting_projects.end()) {
                    ++discarded;
                    return;
                }

                if (contentsId == FILE_DELETED) {
                    ++discarded;
                    return;
                }

                ++kept;

                CommitInfo commitInfo;
                commitInfo.projectId = projectId;
                commitInfo.commitId = commitId;
                commitInfo.contentsId = contentsId;
                commitInfo.pathId = pathId;
                commitInfo.path = ids_of_paths_to_interesting_package_json.at(pathId);

                interesting_commits.push_back(commitInfo);
            });

            std::cerr << "Kept " << kept << " commits" << std::endl;
            std::cerr << "Discarded " << discarded << " commits" << std::endl;

            helpers::FinishTask(task, timer);
        }

        void LoadHashes(std::unordered_map<unsigned, std::string> &hashes) {
            clock_t timer = clock();
            std::string task = "loading (all) hashes";
            helpers::StartTask(task, timer);

            new HashToIdLoader([&](unsigned id, std::string const &hash){
                assert(hashes.find(id) == hashes.end());
                hashes[id] = hash;
            });

            helpers::FinishTask(task, timer);
        }

        void PrepareForDownload(std::unordered_map<unsigned, NPMProject *> const &projects,
                                std::unordered_map<unsigned, std::string> const &hashes,
                                std::vector<CommitInfo> const &interesting_commits,
                                std::vector<Download> &downloads) {

            clock_t timer = clock();
            size_t inspected_commits = 0;

            std::string task = "preparing list of files to download";
            helpers::StartTask(task, timer);

            for (CommitInfo commit : interesting_commits) {
                NPMProject *project = projects.at(commit.projectId);
                std::string hash = hashes.at(commit.commitId);

                std::stringstream url;
                url << "https://raw.githubusercontent.com/"
                    << project->user << "/" << project->repo << "/"
                    << hash << "/" << commit.path;

                std::stringstream dir;
                dir << "/package.json/" << project->id << "/"
                    << commit.pathId << "/";

                std::stringstream file;
                file << commit.contentsId;

                Download download(url.str(), dir.str(), file.str());
                downloads.push_back(download);

                helpers::Count(inspected_commits);
            }

            std::cerr << "Files to download:" << inspected_commits << std::endl;
            helpers::FinishTask(task, timer);
        }
//
//        void SaveDownload(std::vector<Download *> const &downloads) {
//            std::string filename = DataDir.value() + "/package.json/__all.csv";
//
//            clock_t timer = clock();
//            std::string task = "saving list of stuff to download";
//            size_t items = 0;
//            helpers::StartCounting(items);
//            helpers::StartTask(task, timer);
//
//            std::ofstream s(filename);
//            if (! s.good()) {
//                ERROR("Unable to open file " << filename << " for writing");
//            }
//
//            s << "url,filename" << std::endl;
//            for (Download *download : downloads) {
//                s << DataDir.value() << "/" << download->path << ","
//                  << download->url << std::endl;
//                helpers::Count(items);
//            }
//
//            s.close();
//
//            helpers::FinishCounting(items);
//            helpers::FinishTask(task, timer);
//        }

        void DownloadAll(std::vector<Download> const &downloads) {
            std::string filename = DataDir.value() + "/package.json/__failed.csv";

            clock_t timer = clock();
            std::string task = "downloading stuff";
            size_t downloaded = 0;
            size_t failed = 0;
            size_t attempted = 0;
            helpers::StartTask(task, timer);

            std::ofstream s(filename);
            if (! s.good()) {
                ERROR("Unable to open file " << filename << " for writing");
            }
            s << "url,dir,file" <<std::endl;

            for (Download const &download : downloads) {
                std::stringstream mkdirPath;
                mkdirPath << DataDir.value() + "/" + download.dir;

                std::stringstream wgetCmd;
                wgetCmd << "wget -nv -q "
                        << "-O " << DataDir.value() << "/" << download.path
                        << " " << download.url;

                int status = mkdir(mkdirPath.str().c_str(),
                                   S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
                assert(status == 0);

                status = system(wgetCmd.str().c_str());
                if (status != 0) {
                    s << download.url << "," << download.dir << "," << download.file << std::endl;
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

        std::unordered_set<unsigned> interesting_projects;
        std::unordered_set<std::string> paths_to_interesting_package_json;
        std::unordered_map<unsigned, std::unordered_set<std::string>> interesting_projects_and_paths_to_package_json;
        LoadInterestingProjectsAndPaths(interesting_projects,
                                        paths_to_interesting_package_json,
                                        interesting_projects_and_paths_to_package_json);

        std::unordered_map<unsigned, std::string> ids_of_paths_to_interesting_package_json;
        LoadIDsOfInterestingPaths(paths_to_interesting_package_json,
                                  ids_of_paths_to_interesting_package_json);

        std::unordered_map<unsigned, NPMProject *> interesting_projects_info;
        LoadInfoForInterestingProjects(interesting_projects,
                                       interesting_projects_info);

        std::unordered_map<unsigned, std::string> hashes;
        LoadHashes(hashes);

        std::vector<CommitInfo> interesting_commits;
        LoadCommitsThatChangeProjectJSONPathsInInterestingProjects(interesting_projects,
                                                                   ids_of_paths_to_interesting_package_json,
                                                                   interesting_commits);

        std::vector<Download> downloads;
        PrepareForDownload(interesting_projects_info,
                           hashes,
                           interesting_commits,
                           downloads);

//        SaveDownload(downloads);

        DownloadAll(downloads);
    }
    
} // namespace dejavu
