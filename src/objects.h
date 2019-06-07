#pragma once

#include <sstream>

#include <unordered_set>
#include <vector>
#include <unordered_map>

namespace dejavu {

    constexpr unsigned UNKNOWN_HASH = -1;
    constexpr unsigned FILE_DELETED = 0;
    constexpr unsigned EMPTY_PATH = 0;

    /** Memory efficient implementation of the SHA1 hash used in the algorithms.

        Uses 20 bytes of memory, allows printing and can be used as index in associative containers.
     */
    class SHA1Hash {
    public:
        unsigned char hash[20];

        std::string toString() const {
            std::stringstream s;
            s << std::hex;
            for (size_t i = 0; i < 20; ++i)
                s << (unsigned) hash[i];
            return s.str();
        }

        bool operator == (SHA1Hash const & other) const {
            for (size_t i = 0; i < 20; ++i)
                if (hash[i] != other.hash[i])
                    return false;
            return true;
        }

        friend std::ostream & operator << (std::ostream & o, SHA1Hash const & hash) {
            o << hash.toString();
            return o;
        }
    };
    
    template<typename PROJECT,typename COMMIT, typename COMMIT_SET = std::unordered_set<COMMIT *>>
    class BaseProject {
    public:
        unsigned id;
        uint64_t createdAt;

        COMMIT_SET commits;

        // commit iterator interface

        bool hasCommit(COMMIT * c) const {
            return commits.find(c) != commits.end();
        }

        typename COMMIT_SET::iterator commitsBegin() {
            return commits.begin();
        }

        typename COMMIT_SET::iterator commitsEnd() {
            return commits.end();
        }
        
        // methods
        
        void addCommit(COMMIT * c) {
            commits.insert(c);
        }

        // comparator
        
        BaseProject(unsigned id, uint64_t createdAt):
            id(id),
            createdAt(createdAt) {
        }

        // various comparators of projects

        struct ByAge {
            bool operator()(PROJECT * first, PROJECT * second) const {
                if (first->createdAt < second->createdAt)
                    return true;
                if (first->createdAt == second->createdAt)
                    return first < second;
                return false;
            }
        };

        struct ByAgeDesc {
            bool operator()(PROJECT * first, PROJECT * second) const {
                if (first->createdAt > second->createdAt)
                    return true;
                if (first->createdAt == second->createdAt)
                    return first > second;
                return false;
            }
        };

        struct ByNumCommits {
            bool operator()(PROJECT * first, PROJECT * second) const {
                if (first->commits.size() > second->commits.size())
                    return true;
                if (first->commits.size() == second->commits.size())
                    return first < second;
                return false;
            }
        };
    };

    template<typename COMMIT>
    class BaseCommit {
    public:
        unsigned id;
        uint64_t time;
        std::unordered_map<unsigned, unsigned> changes;
        std::unordered_set<unsigned> deletions;

        std::vector<COMMIT *> children;
        std::vector<COMMIT *> parents;

        // interface for Commits iterator
        
        std::vector<COMMIT *> const & childrenCommits() const {
            return children;
        }

        unsigned numParentCommits() const {
            return parents.size();
        }

        // basic access methods

        void addParent(COMMIT * c) {
            parents.push_back(c);
            c->children.push_back(reinterpret_cast<COMMIT*>(this));
        }

        void addChange(unsigned path, unsigned contents) {
            if (contents != FILE_DELETED)
                changes.insert(std::make_pair(path, contents));
            else
                deletions.insert(path);
        }

        // constructor
        
        BaseCommit(unsigned id, uint64_t time):
            id(id),
            time(time) {
        }
        
    };



    
} // namespace dejavu

namespace std {
    template<>
    struct hash<dejavu::SHA1Hash> {
        size_t operator()(dejavu::SHA1Hash const &hash) const {
            size_t result = 0;
            result += *(uint32_t*)(hash.hash);
            result += *(uint32_t*)(hash.hash + 4);
            result += *(uint32_t*)(hash.hash + 8);
            result += *(uint32_t*)(hash.hash + 12);
            result += *(uint32_t*)(hash.hash + 16);
            return result;
        }
    };
    
}
