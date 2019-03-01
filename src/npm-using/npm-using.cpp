#include "helpers/helpers.h"
#include "../settings.h"
#include "src/objects.h"
#include "npm-using.h"

#include <dirent.h>
#include <string>

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed",
                                               false);
        helpers::Option<std::string> NodeDir("nodeDir", "/data/dejavuii/node-projects",
                                             false);
        helpers::Option<std::string> ProjectDir("projectDir", "/processed",
                                                false);
    } // anonymous namespace

    std::vector<std::string> read_directory(std::string path, bool prefix) {
        DIR* dir = opendir(path.c_str());
        std::vector<std::string> result;
        struct dirent * dp;
        while ((dp = readdir(dir)) != NULL) {
            std::string const file = dp->d_name;
            if ("." == file || ".." == file)
                continue;
            result.push_back(prefix ? path + "/" + file : file);
        }
        closedir(dir);
        return result;
    }

    int hex_string_to_int(const std::string & code) {
        unsigned int hex_value;
        std::stringstream converter_stream;
        converter_stream << std::hex << code;
        converter_stream >> hex_value;
        return hex_value;
    }

    std::string code_to_character(const std::string & code) {
        assert(code.size() == 2);
        int hex_value = hex_string_to_int(code);
        std::string character(1, static_cast<char>(hex_value));
        return character;
    }

    std::string decode_string(const std::string & encoded) {
        std::stringstream decoded;
        for (size_t i = 0; i < encoded.size();) {
            char c = encoded[i];
            if (c == '_') {
                assert(encoded.size() - i - 1 >= 2);
                std::string code({encoded[i+1], encoded[i+2]});
                decoded << code_to_character(code);
                i+=3;
            }  else {
                decoded << c;
                ++i;
            }
        }
        return decoded.str();
    }

    std::pair<std::string, std::string> to_owner_and_project(std::string const & s) {
        std::string decoded_string = decode_string(s);
        std::vector<std::string> parts = helpers::split(decoded_string, '/', 2);
        assert(parts.size() == 2);
        return std::make_pair(parts[0], parts[1]);
    }

    std::vector<std::string> * make_npm_project_list() {
        std::cerr << "  NAO I MAK NPM PROJEKT LIST" << std::endl;
        std::vector<std::string> * npm_projects = new std::vector<std::string>();
        unsigned int inspected = 0;
        for (auto path : read_directory(NodeDir.value(), true)) {
            for (auto file : read_directory(path, false)) {
                ++inspected;
                std::string repository = decode_string(file);
                std::cerr << "  I INSPEKTEDZ " << inspected << " FILEZ" << "\r";
                npm_projects->push_back(repository);
            }
        }
        std::cerr << std::endl;
        std::cerr << "  NAO I HAV NPM PROJEKT LIST" << std::endl;
        return npm_projects;
    }

    std::unordered_map<std::string, unsigned> * make_project_id_map() {
        std::cerr << "  NAO I MAK PROJEKT ID MAP" << std::endl;
        unsigned int n_projects = 0;
        std::unordered_map<std::string, unsigned> * projects =
                new std::unordered_map<std::string, unsigned>();
        for (auto project_entry : Project::AllProjects()) {
            ++projects;
            unsigned project_id = project_entry.first;
            std::string repository = project_entry.second->user
                                     + "/" + project_entry.second->repo;
            std::cerr << "  I MAPD " << n_projects << " PROJEKT IDZ " << "\r";
            (*projects)[repository] = project_id;
        }
        std::cerr << std::endl;
        std::cerr << "  NAO I HAV PROJEKT ID MAP" << std::endl;
        return projects;
    }

    void combine_and_output(std::unordered_map<std::string, unsigned> * project_ids, std::vector<std::string> * npm_projects) {
        std::cerr << "  NAO I COMBINE ALL AND WRITEZ CSV FILE " << std::endl;
        std::string output_path(DataRoot.value() + OutputDir.value() + "/npm-using.csv");
        std::ofstream csv_file(output_path);
        if (! csv_file.good())
            ERROR("Unable to open file " << output_path << " for writing");
        csv_file << "\"repository\",\"project_id\"" << std::endl;
        unsigned int n_projects = 0;
        for (std::string project : (*npm_projects)) {
            ++n_projects;

            auto it = project_ids->find(project);
            assert(it != project_ids->end());
            unsigned project_id = it->second;

            std::cerr << "  I WRITED " << n_projects << " PROJEKTZ " << "\r";

            csv_file << "\"" << project << "\"," << project_id << std::endl;
        }
        std::cerr << std::endl;
        std::cerr << "  NAO I DONE " << std::endl;
    }

    void NPMUsing(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(NodeDir);
        settings.addOption(OutputDir);
        settings.addOption(ProjectDir);
        settings.parse(argc, argv);
        settings.check();

        Project::ImportFrom(DataRoot.value() + ProjectDir.value() + "/projects.csv", false);

        std::unordered_map<std::string, unsigned> * project_ids = make_project_id_map();
        std::vector<std::string> * npm_projects = make_npm_project_list();

        combine_and_output(project_ids, npm_projects);
    }
}