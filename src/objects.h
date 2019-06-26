#pragma once

#include <cassert>

#include <sstream>

#include <unordered_set>
#include <vector>
#include <unordered_map>

namespace dejavu {

    constexpr unsigned UNKNOWN_HASH = -1;
    constexpr unsigned FILE_DELETED = 0;
    constexpr unsigned EMPTY_PATH = 0;
    constexpr unsigned NO_FORK = std::numeric_limits<unsigned>::max();
    constexpr unsigned UNKNOWN_FORK = std::numeric_limits<unsigned>::max() - 1;

    inline uint64_t Join2Unsigned(unsigned a, unsigned b) {
        return (static_cast<uint64_t>(a) << 32) + b;
    }

    inline std::pair<unsigned, unsigned> Decouple2Unsigned(uint64_t value) {
        return std::make_pair(static_cast<unsigned>(value >> 32), static_cast<unsigned>(value & 0xffffffff));
    }


    /** To make sure that the originals are deterministic, this function should be used for checking whether an original value should be updated.

        It expects project to support createdAt field and commit to support time field. 
     */
    template<typename PROJECT, typename COMMIT>
        bool IsBetterOriginal(PROJECT * originalProject, COMMIT * originalCommit, std::string const & originalPath, PROJECT * project, COMMIT * commit, std::string const & path) {
        // if the new commit is younger then it is better candidateOB
        if (commit->time < originalCommit->time)
            return true;
        if (commit->time == originalCommit->time) {
            // if the commit times are identical then if the new project is older, it is a better candidate
            if (project->createdAt < originalProject->createdAt)
                return true;
            if (project->createdAt == originalProject->createdAt) {
                // if the projects have the same age, then if one of the commits has smaller id, it is better candidate (just to disambiguate)
                if (commit->id < originalCommit->id)
                    return true;
                // and finally, if even the commit is the same, the we use lexically smaller path
                if (commit->id == originalCommit->id)
                    return path < originalPath;
            }
        }
        return false;
    }

    /** Memory efficient implementation of the SHA1 hash used in the algorithms.

        Uses 20 bytes of memory, allows printing and can be used as index in associative containers.
     */
    class SHA1Hash {
    public:
        unsigned char hash[20];

        std::string toString() const {
            std::stringstream s;
            s << std::hex;
            for (size_t i = 0; i < 20; ++i) {
                if (hash[i] < 16)
                    s << '0';
                s << (unsigned) hash[i];
            }
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

        static SHA1Hash FromHexString(std::string const & str) {
            SHA1Hash result;
            assert(str.size() == 40);
            for (auto i = 0; i < 20; ++i) 
                result.hash[i] = HexDigitToValue(str[i * 2]) * 16 + HexDigitToValue(str[i * 2 + 1]);
            return result;
        }
    private:

        static unsigned char HexDigitToValue(char what) {
            if (what >= '0' && what <= '9')
                return what - '0';
            if (what >= 'a' && what <= 'f')
                return what - 'a' + 10;
            assert(false);
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

    template<typename PROJECT,typename COMMIT, typename COMMIT_SET = std::unordered_set<COMMIT *>>
    class FullProject : public BaseProject<PROJECT, COMMIT, COMMIT_SET> {
    public:
        std::string user;
        std::string repo;
        FullProject(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
            BaseProject<PROJECT,COMMIT,COMMIT_SET>(id, createdAt),
            user(user),
            repo(repo) {
        }
    };

    template<typename COMMIT>
    class BaseCommit {
    public:
        unsigned id;
        uint64_t time;
        // pathId -> contentsId
        std::unordered_map<unsigned, unsigned> changes;
        // pathId
        std::unordered_set<unsigned> deletions;

        std::unordered_set<COMMIT*> children;
        std::unordered_set<COMMIT*> parents;

        // interface for Commits iterator
        
        std::unordered_set<COMMIT *> const & childrenCommits() const {
            return children;
        }

        unsigned numParentCommits() const {
            return parents.size();
        }

        // basic access methods

        void addParent(COMMIT * c) {
            parents.insert(c);
            c->children.insert(reinterpret_cast<COMMIT*>(this));
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

    template<typename COMMIT>
    class BaseDummyState {
    public:
        BaseDummyState() {
        }

        BaseDummyState(BaseDummyState const &) {
        }

        void mergeWith(BaseDummyState const &, COMMIT *) {
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
