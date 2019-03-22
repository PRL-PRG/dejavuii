#include "helpers/helpers.h"
#include "src/settings.h"
#include "src/objects.h"
#include "npm-packages.h"
#include "npm-common.h"

#include "helpers/json.hpp"

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed",
                                               false);
        helpers::Option<std::string> NPMDir("nodeDir", "/data/dejavuii/npm-packages",
                                            false);
        helpers::Option<std::string> ProjectDir("projectDir", "/processed",
                                                false);
    } // anonymous namespace

    std::string url_to_repository(std::string const & url) {
        std::vector<std::string> segments = helpers::split(url, '/');
        size_t size = segments.size();

        if (url.find("github.com") == std::string::npos) {
            std::cerr << "apparently not a github url: " << url << std::endl;
            return "";
        }

        if (size < 2) {
            std::cerr << "invalid credentials for url: " << std::endl;
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

    void extract_data_from_repository(nlohmann::json json, std::vector<std::pair<std::string, std::string>> & repos) {
        if (json.is_string()) {
            std::string url = json.get<std::string>();
            std::string repo_info = url_to_repository(url);
            if (repo_info != "" || url != "") {
                repos.push_back(std::make_pair(repo_info, url));
            }
        }
        if (json.is_object()) {
            auto type = json["type"];
            if (type.is_string()) {
                if (type.get<std::string>() == "git") {
                    if (json["url"].is_string()) {
                        auto url = json["url"].get<std::string>();
                        std::string repo_info = url_to_repository(url);
                        if (repo_info != "" || url != "") {
                            repos.push_back(std::make_pair(repo_info, url));
                        }
                    }
                }
            }
        }
    }

    std::vector<std::pair<std::string, std::string>> extract_from_json(std::string file) {
        std::cerr << "opening " << file << std::endl;
        std::vector<std::pair<std::string, std::string>> repos;

        try {
            nlohmann::json data;

            std::ifstream input(file);
            assert(input.is_open());
            input >> data;

            for (auto version : data["versions"]) {

                if (!version.is_object()) {
                    auto repository = data["repository"];
                    extract_data_from_repository(repository, repos);
                    continue;
                }

                auto package_json = version["package_json"];
                auto repository = package_json["repository"];

                extract_data_from_repository(repository, repos);
            }
            return repos;

        } catch (nlohmann::json::parse_error& e) {
            std::cerr << "could not parse: " << file
                      << " ("<< e.what() << ")" << std::endl;
            return repos;
        }
    }

    std::vector<std::string> filter_files_by_extension(std::vector<std::string> files, std::string extension = ".json") {
        std::vector<std::string> result;
        std::copy_if (files.begin(), files.end(),
                      std::back_inserter(result),
                      [extension] (std::string s){
                          if (s.size() < extension.size())
                                  return false;
                          return std::equal(extension.rbegin(),
                                            extension.rend(),
                                            s.rbegin());
                      });
        return result;
    }

    void fill_npm_package_list(std::vector<std::pair<std::string, std::string>> & npm_projects) {
        std::cerr << "NAO I MAK NPM PACKAGE LIST FROM " << NPMDir.value() << std::endl;
        unsigned int inspected = 0;

        std::vector<std::string> files = filter_files_by_extension(
                helpers::read_directory_recursive(NPMDir.value()));

        for(std::string file : files) {
            std::cerr << " file " << file << std::endl;
            ++inspected;
            std::vector<std::pair<std::string, std::string>> repos =
                    extract_from_json(file);

            npm_projects.insert(npm_projects.end(), repos.begin(),
                                repos.end());

            if (inspected % 1000 == 0)
                std::cerr << "    I INSPEKTEDZ " << inspected / 1000
                          << "K FILEZ" << "\r";
        }

        std::cerr << std::endl;
        std::cerr << "NAO I HAV NPM PACKAGE LIST" << std::endl;
    }

    std::string npm_packages_header() {
        return "\"repository\",\"project_id\",\"url\"";
    }

    std::string npm_packages_formatter(std::pair<std::string, std::string> repo,
                                       unsigned project_id) {
        return STR("\"" << std::get<0>(repo) << "\","
                        << project_id << ","
                        << "\"" << std::get<1>(repo) << "\"");
    }

    std::string npm_packages_formatter_NA(std::pair<std::string, std::string> repo) {
        return STR("\"" << std::get<0>(repo) << "\","
                        << ","
                        << "\"" << std::get<1>(repo) << "\"");
    }

    void NPMPackages(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(NPMDir);
        settings.addOption(OutputDir);
        settings.addOption(ProjectDir);
        settings.parse(argc, argv);
        settings.check();

        std::string output_path(DataRoot.value() + OutputDir.value() + "/npm-packages.csv");

        Project::ImportFrom(DataRoot.value() + ProjectDir.value() + "/projects.csv", false);

        std::unordered_map<std::string, unsigned> project_ids;
        std::vector<std::pair<std::string,std::string>> npm_packages;

        fill_project_id_map(project_ids);
        fill_npm_package_list(npm_packages);

        combine_and_output(output_path, project_ids, npm_packages);
    }
}
