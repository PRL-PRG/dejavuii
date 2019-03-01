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

    void NPMUsing(int argc, char * argv[]) {
        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(NodeDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();

        std::string output_path("X");
        std::ofstream csv_file(output_path);

        if (! csv_file.good())
            ERROR("Unable to open file " << output_path << " for writing");

        csv_file << "\"owner\",\"project\"" << std::endl;
        for (auto path : read_directory(NodeDir.value(), true)) {
            for (auto file : read_directory(path, false)) {
                std::cerr << file << std::endl;
                std::pair<std::string, std::string> repo = to_owner_and_project(file);
                csv_file << "\"" << repo.first << "\",\"" << repo.second
                         << "\"" << std::endl;
            }
        }
    }
}