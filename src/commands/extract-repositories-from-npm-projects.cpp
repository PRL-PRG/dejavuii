#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>

#include "../loaders.h"
#include "helpers/json.hpp"

namespace dejavu {

    namespace {
        helpers::Option<std::string> NPMDir("NPMDir", "/data/dejavuii/npm-packages", false);
    };

    struct Project {
        unsigned id;
        std::string user;
        std::string repo;
    };

    struct NPMPackage {
        std::string repo;
        std::string url;
        bool github;
    };

    void LoadProjects(std::unordered_map<unsigned, Project> &projects,
                      std::unordered_map<std::string, unsigned> &project_ids) {
        clock_t timer = clock();
        std::string task = "loading projects and projects IDs";
        helpers::StartTask(task, timer);

        ProjectLoader([&](unsigned id, std::string const & user,
                          std::string const & repo, uint64_t createdAt){

            projects[id].id = id;
            projects[id].user = user;
            projects[id].repo = repo;

            project_ids[user + "/" + repo] = id;
        });

        helpers::FinishTask(task, timer);
    }

    void LoadAllJSONFiles(std::vector<std::string> &json_files) {
        clock_t timer = clock();
        std::string task = "loading JSON files";
        helpers::StartTask(task, timer);

        std::vector<std::string> all_files =
                helpers::ReadDirectoryRecursive(NPMDir.value());

        std::string extension = ".json";

        std::copy_if (all_files.begin(), all_files.end(),
                      std::back_inserter(json_files),
                      [extension] (std::string s){
                          if (s.size() < extension.size()) {
                              return false;
                          }
                          return std::equal(extension.rbegin(),
                                            extension.rend(),
                                            s.rbegin());
                      });

        helpers::FinishTask(task, timer);
    }

    bool IsURLAGitHubRepo(std::string const & url) {
        if (url.find("github.com") != std::string::npos) {
            return true;
        }
        if (url.find("github:") != std::string::npos) {
            return true;
        }
        return false;
    }

    std::string URLToRepository(std::string const & url) {
        std::vector<std::string> segments = helpers::Split(url, '/');
        size_t size = segments.size();

//        if (url.find("github.com") == std::string::npos) {
//            errors.push_back("not a github url: " + url);
//            return "";
//        }

        if (size < 2) {
            std::cerr << "invalid credentials for url: " << url << std::endl;
            return "";
        }

        std::string project = segments[size - 1];
        /* Remove .git from the end, if it's there */ {
            size_t p_size = project.size();
            if (p_size >= 4) {
                std::string tail = project.substr(p_size - 4, p_size - 1);
                if(tail == ".git") {
                    project = project.substr(0, p_size - 4);
                }
            }
        }

        std::string user = segments[size - 2];
        /* Remove www.github.com: from the beginning of the project name */ {
            size_t pos = user.find_first_of(':');
            if (pos != std::string::npos) {
                user = user.substr(pos + 1, user.size() - 1);
            }
        }

        //std::cerr << "::: " << url << " => " << user << "/" << project << std::endl;
        return user + "/" + project;
    }

    void ExtractDataFromRepository(nlohmann::json json,
                                   std::vector<NPMPackage> &repos) {
        if (json.is_string()) {
            std::string url = json.get<std::string>();
            std::string repo_info = URLToRepository(url);
            if (repo_info != "" || url != "") {
                NPMPackage package;
                package.repo = repo_info;
                package.url = url;
                package.github = IsURLAGitHubRepo(url);
                repos.push_back(package);
            }
        }
        if (json.is_object()) {
            auto type = json["type"];
            if (type.is_string()) {
                if (type.get<std::string>() == "git") {
                    if (json["url"].is_string()) {
                        auto url = json["url"].get<std::string>();
                        std::string repo_info = URLToRepository(url);
                        if (repo_info != "" || url != "") {
                            NPMPackage package;
                            package.repo = repo_info;
                            package.url = url;
                            package.github = IsURLAGitHubRepo(url);
                            repos.push_back(package);
                        }
                    }
                }
            }
        }
    }

    std::vector<NPMPackage> ExtractFromJSONFile(std::string file) {
        //std::cerr << "opening " << file << std::endl;
        std::vector<NPMPackage> repos;

        try {
            nlohmann::json data;

            std::ifstream input(file);
            assert(input.is_open());
            input >> data;

            for (auto version : data["versions"]) {

                if (!version.is_object()) {
                    auto repository = data["repository"];
                    ExtractDataFromRepository(repository, repos);
                    continue;
                }

                auto package_json = version["package_json"];
                auto repository = package_json["repository"];

                ExtractDataFromRepository(repository, repos);
            }
            return repos;

        } catch (nlohmann::json::parse_error& e) {
            std::cerr << "could not parse: " << file
                      << " ("<< e.what() << ")" << std::endl;
            return repos;
        }
    }

    void CompilePackageList(std::vector<std::string> const &json_files,
                            std::vector<NPMPackage> &npm_packages) {
        clock_t timer = clock();
        std::string task = "compiling package list form JSON files";
        helpers::StartTask(task, timer);

        size_t inspected = 0;
        helpers::StartCounting(inspected);

        for(std::string file : json_files) {
            std::vector<NPMPackage> repos = ExtractFromJSONFile(file);
            npm_packages.insert(npm_packages.end(), repos.begin(), repos.end());

            helpers::Count(inspected);
        }

        helpers::FinishCounting(inspected, "JSON files");
        helpers::FinishTask(task, timer);
    }

    void SaveRepositoryAndURLInfo(std::unordered_map<std::string, unsigned> const &project_ids,
                                  std::vector<NPMPackage> const &npm_packages) {

        std::string csv_output_path(DataDir.value() + OutputDir.value() + "/npm-packages-urls.csv");
        std::string todo_output_path(DataDir.value() + OutputDir.value() + "/npm-packages-missing.list");

        clock_t timer = clock();
        std::string task = "compiling output and saving to disk";
        helpers::StartTask(task, timer);

        size_t counter = 0;
        helpers::StartCounting(counter);

        size_t packages_with_project_ids = 0;
        size_t packages_without_project_ids = 0;
        size_t packages_with_project_ids_github = 0;
        size_t packages_without_project_ids_github = 0;

        std::ofstream csv_file(csv_output_path);
        if (!csv_file.good()) {
            ERROR("Unable to open file " << csv_output_path << " for writing");
            return;
        }

        std::ofstream todo_file(todo_output_path);
        if (!todo_file.good()) {
            ERROR("Unable to open file " << todo_output_path << " for writing");
            return;
        }

        csv_file << "\"repository\",\"project_id\",\"url\",\"github_repo\"" << std::endl;

        for (NPMPackage package : npm_packages) {
            auto it = project_ids.find(package.repo);
            if (it != project_ids.end()) {
                ++packages_with_project_ids;
                if(package.github) {
                    ++packages_with_project_ids_github;
                }
                unsigned project_id = it->second;
                csv_file << "\"" << package.repo << "\","
                         << project_id << ","
                         << "\"" << package.url << "\","
                         << "\"" << (package.github ? "T" : "F") << "\""
                         << std::endl;
            } else {
                ++packages_without_project_ids;
                if(package.github) {
                    ++packages_without_project_ids_github;
                }
                csv_file << "\"" << package.repo << "\","
                         << /* NA */ ","
                         << "\"" << package.url << "\","
                         << "\"" << (package.github ? "T" : "F") << "\""
                         << std::endl;
                todo_file << package.repo << std::endl;
            }
            helpers::Count(counter);
        }

        std::cerr << "Packages with project IDs: "
                  << packages_with_project_ids
                  << " including confirmed GitHub projects: "
                  << packages_with_project_ids_github
                  << std::endl;

        std::cerr << "Packages without project IDs: "
                  << packages_without_project_ids
                  << " including confirmed GitHub projects: "
                  << packages_without_project_ids_github
                  << std::endl;

        helpers::FinishCounting(counter, "NPM packages");
        helpers::FinishTask(task, timer);
    }

    void ExtractRepositoriesFromNPMProjects(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(NPMDir);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_map<unsigned, Project> projects;
        std::unordered_map<std::string, unsigned> project_ids;
        LoadProjects(projects, project_ids);

        std::vector<std::string> json_files;
        LoadAllJSONFiles(json_files);

        std::vector<NPMPackage> npm_packages;
        CompilePackageList(json_files, npm_packages);

        SaveRepositoryAndURLInfo(project_ids, npm_packages);
    }

};