#pragma once

#ifndef __HELPERS
#define __HELPERS

#include <ctime>
#include <cassert>
#include <functional>
#include <iostream>
#include <chrono>

#include <sys/stat.h>
#include <dirent.h>

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

    inline void System(std::string const & cmd) {
        int err = system(cmd.c_str());
        if (err == -1)
            throw std::runtime_error(STR("Command failed: " + cmd));
    }


    inline bool FileExists(std::string const & path) {
        struct stat x;
        return (stat (path.c_str(), &x) == 0);
    }

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


    /** A simple temporary directory which deletes itself when destroyed.
     */
    class TempDir {
    public:
        TempDir(std::string const & where):
            path_(CreateUniqueTempDir(where)) {
        }

        TempDir():
            path_(CreateUniqueTempDir("")) {
        }

        TempDir(TempDir const &) = delete;

        TempDir(TempDir && t):
            path_(std::move(t.path_)) {
            t.path_ = "";
        }

        ~TempDir() {
            if (!path_.empty()) 
                System(STR("rm -r " << path_));
        }

        std::string const & path() const {
            return path_;
        }

        static std::string CreateUniqueTempDir(std::string const & where) {
            std::string result = Exec("mktemp -d -p " + where, "");
            // drop the trailing \n
            return result.substr(0, result.size() - 1);
        }

    private:

        std::string path_;
    }; // helpers::TempDir



    // C++17 you are sorely missed
    inline size_t DirectoryReader(std::string const & dir, std::function<void(std::string const &)> handler, bool recursive = false) {
        DIR * d = opendir(dir.c_str());
        if (d == nullptr)
            throw std::runtime_error(STR("Unable to open directory " << dir));
        struct dirent * ent;
        size_t numFiles = 0;
        while ((ent = readdir(d)) != nullptr) {
            std::string name = ent->d_name;
            if (name == "." || name == "..")
                continue;
            if (ent->d_type == DT_DIR) {
                if (recursive)
                    numFiles += DirectoryReader(STR(dir << "/" << name), handler, true);
                continue;
            }
            // not every filesystem supports the d_type so we must assume DT_UNKNOWN is also valid
            if (ent->d_type == DT_REG || ent->d_type == DT_UNKNOWN) {
                handler(STR(dir << "/" << name));
                ++numFiles;
            }
        }
        closedir(d);
        return numFiles;
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

    inline void StartTask(const std::string &task, clock_t &timer) {
        timer = clock();
        std::cerr << "Started " << task << std::endl;
    }

    inline void FinishTask(const std::string &task, clock_t &timer) {
        clock_t end = clock();
        std::cerr << "Finished " << task
                  << " in " << (double(end - timer) / CLOCKS_PER_SEC) << "s"
                  << std::endl << std::endl;
    }

    inline void StartCounting(size_t &counter) {
        counter = 0;
    }

    inline void Count(size_t &counter) {
        ++counter;
        if (counter % 1000 == 0) {
            std::cerr << " : " << (counter / 1000) << "k\r" << std::flush;
        }
    }

    inline void FinishCounting(size_t counter) {
        std::cerr << "Iterated over " << counter << " items" << std::endl;
    }

    inline void FinishCounting(size_t counter, std::string items_name) {
        std::cerr << "Iterated over " << counter << " " << items_name
                  << std::endl;
    }

    inline std::vector<std::string> ReadDirectoryRecursive(std::string root, bool include_directories=false) {
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

    /** Returns a steady clock time in milliseconds. 
	    Note that the time has no meaningful reference, so all it is good for is simple time duration calculations. 
	 */
	inline size_t SteadyClockMillis() {
		using namespace std::chrono;
		return static_cast<size_t>(
			duration<long, std::milli>(
				duration_cast<milliseconds>(
					steady_clock::now().time_since_epoch()
					)
				).count()
			);
	}

    
} // namespace helpers

#endif //__HELPERS
