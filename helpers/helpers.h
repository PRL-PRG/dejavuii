#pragma once

#include <memory>
#include <cstdlib>
#include <sstream>

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

    
    
} // namespace helpers
