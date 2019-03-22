#pragma once

#ifndef __HELPERS
#define __HELPERS

#include <ctime>

#include <memory>
#include <cstdlib>
#include <sstream>
#include <vector>
#include <dirent.h>
#include <string>

#define STR(WHAT) static_cast<std::stringstream &&>(std::stringstream() << WHAT).str()

#define STRLN(WHAT) static_cast<std::stringstream &&>(std::stringstream() << WHAT << std::endl).str()

#define NOT_IMPLEMENTED throw std::runtime_error(STR("NOT IMPLEMENTED: " << __FILE__ << "[" << __LINE__ << "]"))

#define UNREACHABLE throw std::runtime_error(STR("UNREACHABLE: " << __FILE__ << "[" << __LINE__ << "]"))

#define ERROR(...) throw std::runtime_error(STR(__VA_ARGS__))

namespace helpers {


    /** Creates given directory including all subdirs.
     */
    inline void EnsurePath(std::string const & path) {
        int err = system(STR("mkdir -p " << path).c_str());
        if (err == -1)
            throw std::runtime_error(STR("Unable to create directory " + path));
    }

    /** A simple temporary directory which deletes itself when destroyed.
     */
    class TempDir {
    public:
        TempDir(std::string const & path):
            path(path) {
            EnsurePath(path);
        }

        ~TempDir() {
            /*int err = */ system(STR("rm -r " << path).c_str());
            //if (err == -1)
            //throw std::runtime_error(STR("Unable to delete temporary directory " + path));
        }

        std::string const  path;
    }; // helpers::TempDir


    /** Executes the gfiven command and returns its output.
        
     */
    inline std::string Exec(std::string const & what, std::string const & path ) {
        char buffer[128];
        std::string result = "";
        std::string cmd = STR("cd \"" << path << "\" && " << what << " 2>&1");
        std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
        if (not pipe)
            throw std::runtime_error(STR("Unable to execute command " << cmd));
        while (not feof(pipe.get())) {
            if (fgets(buffer, 128, pipe.get()) != nullptr)
                result += buffer;
        }
        return result;
    }

    inline std::string ToTime(size_t seconds) {
        unsigned s = seconds % 60;
        seconds = seconds / 60;
        unsigned m = seconds % 60;
        seconds = seconds / 60;
        unsigned h = seconds % 24;
        seconds = seconds / 24;
        return STR(seconds << " days " << h << ":" << m << "." << s);
    }

    inline uint64_t StrToTimestamp(std::string const & s, std::string const & format = "%Y-%m-%d %T") {
        std::tm time;
        strptime(s.c_str(), format.c_str(), &time);
        return std::mktime(&time);
    }

    inline std::vector<std::string> read_directory_recursive(std::string root, bool include_directories=false) {
        std::vector<std::string> paths;
        std::vector<std::string> result;
        paths.push_back(root);

        while (!paths.empty()) {
            std::string path = paths.back();
            paths.pop_back();

            DIR *dir = opendir(path.c_str());
            if (dir != NULL) {
                struct dirent *dp;
                while ((dp = readdir(dir)) != NULL) {
                    std::string const file = dp->d_name;

                    if (dp->d_type == DT_DIR) {
                        if ("." == file || ".." == file) {
                            continue;
                        }
                        paths.push_back(path + "/" + file);
                        if (include_directories) {
                            result.push_back(path + "/" + file);
                        }
                    } else {
                        result.push_back(path + "/" + file);
                    }
                }
                closedir(dir);
            } else {
                //std::cerr << std::endl << "    I IGNOREZ " << path << std::endl;
            }
        }

        return result;
    }

    inline std::vector<std::string> read_directory(std::string path, bool prefix) {
        DIR* dir = opendir(path.c_str());
        std::vector<std::string> result;
        if (dir != NULL) {
            struct dirent *dp;
            while ((dp = readdir(dir)) != NULL) {
                std::string const file = dp->d_name;
                if ("." == file || ".." == file) {
                    continue;
                }
                result.push_back(prefix ? path + "/" + file : file);
            }
            closedir(dir);
        } else {
            //std::cerr << std::endl << "    I IGNOREZ " << path << std::endl;
        }
        return result;
    }
    
} // namespace helpers

#endif //__HELPERS