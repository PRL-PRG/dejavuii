#include "helpers/helpers.h"
#include "../settings.h"
#include "src/objects.h"
#include "npm-using.h"
#include "npm-common.h"

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed",
                                               false);
        helpers::Option<std::string> NodeDir("nodeDir", "/data/dejavuii/node-projects",
                                             false);
        helpers::Option<std::string> ProjectDir("projectDir", "/processed",
                                                false);
    } // anonymous namespace

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

    void fill_node_project_list(std::vector<std::string> & npm_projects) {
        std::cerr << "NAO I MAK NPM PROJEKT LIST" << std::endl;
        unsigned int inspected = 0;
        for (auto path : helpers::read_directory(NodeDir.value(), true)) {
            for (auto file : helpers::read_directory(path, false)) {
                ++inspected;
                std::string repository = decode_string(file);
                npm_projects.push_back(repository);
                if (inspected % 1000 == 0)
                    std::cerr << "    I INSPEKTEDZ " << inspected/1000
                              << "K FILEZ" << "\r";
            }
        }
        std::cerr << std::endl;
        std::cerr << "NAO I HAV NPM PROJEKT LIST" << std::endl;
    }

    void NPMUsing(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(NodeDir);
        settings.addOption(OutputDir);
        settings.addOption(ProjectDir);
        settings.parse(argc, argv);
        settings.check();

        std::string output_path(DataRoot.value() + OutputDir.value() + "/npm.csv");

        Project::ImportFrom(DataRoot.value() + ProjectDir.value() + "/projects.csv", false);

        std::unordered_map<std::string, unsigned> project_ids;
        std::vector<std::string> npm_projects;

        fill_project_id_map(project_ids);
        fill_node_project_list(npm_projects);

        combine_and_output(output_path, project_ids, npm_projects);
    }
}
