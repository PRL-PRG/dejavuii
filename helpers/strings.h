#pragma once

#include <string>

namespace helpers {

    inline bool startsWith(std::string const & value, std::string const & prefix) {
        return value.find(prefix) == 0;
    }
    
    inline bool endsWith(std::string const & value, std::string const & ending) {
        if (ending.size() > value.size())
            return false;
        return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
    }

    
    inline std::vector<std::string> split(std::string const & what, char delimiter) {
        std::vector<std::string> result;
        size_t start = 0;
        for (size_t i = 0, e = what.size(); i != e; ++i) {
            if (what[i] == delimiter) {
                result.push_back(what.substr(start, i - start));
                start = i + 1;
            }
        }
        result.push_back(what.substr(start, what.size() - start));
        return result;
    }

    inline std::string join(std::vector<std::string> const & vec, std::string by, size_t start = 0, size_t end = 0 ) {
        std::string result = vec[start];
        ++start;
        while (start < end) {
            result = result + by + vec[start++];
        }
        return result;
    }
    
}
