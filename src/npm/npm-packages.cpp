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

        if (size < 2) {
            std::cerr << "::: " << url << " => " << "" << std::endl;
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

    std::vector<std::string> extract_from_json(std::string file) {
        nlohmann::json data;

        std::ifstream input(file);
        assert(input.is_open());
        input >> data;

        std::vector<std::string> repos;

        for (auto version : data["versions"]) {
            auto package_json = version["package_json"];
            auto repository = package_json["repository"];

            if (repository.is_string()) {
                std::string url = repository.get<std::string>();
                std::string repo_info = url_to_repository(url);
                repos.push_back(repo_info);
            }

            if (repository.is_object()) {
                auto type = repository["type"];
                if (type.is_string()) {
                    if (type.get<std::string>() == "git") {
                        if (repository["url"].is_string() ) {
                            auto url = repository["url"].get<std::string>();
                            std::string repo_info = url_to_repository(url);
                            repos.push_back(repo_info);

                        }
                    }
                }
            }
        }
        return repos;
    }

    void fill_npm_package_list(std::vector<std::string> & npm_projects) {
        std::cerr << "NAO I MAK NPM PROJEKT LIST" << std::endl;
        unsigned int inspected = 0;
        std::cerr<<NPMDir.value()<<std::endl;
        for (auto path : helpers::read_directory(NPMDir.value(), true)) {
            for (auto dir : helpers::read_directory(path, true)) {
                for (auto file : helpers::read_directory(dir, true)) {
                    //std::cerr << file << " --> "; // << std::endl;
                    ++inspected;
                    std::vector<std::string> repos = extract_from_json(file);

                    npm_projects.insert(npm_projects.end(), repos.begin(),
                                        repos.end());

                    if (inspected % 1000 == 0)
                        std::cerr << "    I INSPEKTEDZ " << inspected / 1000
                                  << "K FILEZ" << "\r";
                }
            }
        }
        std::cerr << std::endl;
        std::cerr << "NAO I HAV NPM PROJEKT LIST" << std::endl;
    }

    void NPMPackages(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(NPMDir);
        settings.addOption(OutputDir);
        settings.addOption(ProjectDir);
        settings.parse(argc, argv);
        settings.check();

        std::string output_path(DataRoot.value() + OutputDir.value() + "/npm.csv");

        Project::ImportFrom(DataRoot.value() + ProjectDir.value() + "/projects.csv", false);

        std::unordered_map<std::string, unsigned> project_ids;
        std::vector<std::string> npm_packages;

        fill_project_id_map(project_ids);
        fill_npm_package_list(npm_packages);

        combine_and_output(output_path, project_ids, npm_packages);
    }
}
