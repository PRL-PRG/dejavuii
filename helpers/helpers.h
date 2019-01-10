#pragma once

#include <cstdlib>
#include <sstream>

#define STR(WHAT) static_cast<std::stringstream &&>(std::stringstream() << WHAT).str()


namespace helpers {


    /** Creates given directory including all subdirs.
     */
    inline void EnsurePath(std::string const & path) {
        int err = system(STR("mkdir -p " << path).c_str());
        if (err == -1)
            throw std::runtime_error(STR("Unable to create directory " + path));
    }

} // namespace helpers
