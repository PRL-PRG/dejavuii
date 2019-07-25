#pragma once

#include <string>
#include <vector>
#include <algorithm>

namespace helpers {

    /** Converts given character containing a decimal digit to its value.
     */
    inline unsigned DexCharToNumber(char what) {
        return what - '0';
    }

    /** Converts given character containing hexadecimal digit (upper and lower case variants of a-f are supported) to its value.
     */
    inline unsigned HexCharToNumber(char what) {
        if (what <= '9')
            return what - '0';
        if (what <= 'F')
            return what - 'A' + 10;
        return
            what - 'a' + 10;
    }
    
    inline std::string escapeQuotes(std::string const & from) {
        std::string result = "\"";
        for (char c : from) {
            switch (c) {
            case '\'':
            case '"':
            case '\\':
                result = result + '\\' + c;
                break;
            default:
                result += c;
            }
        }
        result = result + "\"";
        return result;
    }
    
    inline bool startsWith(std::string const & value, std::string const & prefix) {
        return value.find(prefix) == 0;
    }
    
    inline bool endsWith(std::string const & value, std::string const & ending) {
        if (ending.size() > value.size())
            return false;
        return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
    }

    inline std::vector<std::string> Split(std::string const & what, char delimiter) {
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

    inline std::vector<std::string> Split(std::string const & what, char delimiter, size_t limit) {
        std::vector<std::string> result;

        if (limit < 2) {
            result.push_back(what);
            return  result;
        }

        size_t start = 0;
        for (size_t i = 0, e = what.size(); i != e; ++i) {
            if (what[i] == delimiter) {
                result.push_back(what.substr(start, i - start));
                start = i + 1;

                if (result.size() == limit - 1) {
                    break;
                }
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

    inline std::string lstrip(std::string s) {
        s.erase(0, s.find_first_not_of("\t\n\v\f\r "));
        return s;
    }

    inline std::string rstrip(std::string s) {
        s.erase(s.find_last_not_of("\t\n\v\f\r ") + 1);
        return s;
    }

    inline std::string strip(std::string s) {
        s.erase(0, s.find_first_not_of("\t\n\v\f\r "));
        s.erase(s.find_last_not_of("\t\n\v\f\r ") + 1);
        return s;
    }

    inline std::string ToLower(std::string const & from) {
        std::string result = from;
        std::transform(result.begin(), result.end(), result.begin(),[](unsigned char c){ return std::tolower(c); });                            
        return result;
    }
}
