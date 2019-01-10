#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_map>

namespace dejavu {

    class Object {
    public:
        unsigned const id;

    protected:
        Object(unsigned id):
            id() {
        }
    };

    class Commit : public Object {
    public:
        Commit(unsigned id, std::string const & hash, uint64_t time):
            Object(id),
            hash(hash),
            time(time) {
            assert(commits_.find(id) == commits_.end() && "Commit already exists");
            commits_[id] = this;
        }

        static void ImportFrom(std::string const & filename);

        std::string const hash;
        uint64_t const time;

    private:

        static std::unordered_map<unsigned, Commit *> commits_;
        
    }; // dejavu::Commit

    class Project : public Object {
    public:

        Project(unsigned id, std::string const & repo, std::string const & user):
            Object(id),
            repo(repo),
            user(user) {
            assert(projects_.find(id) == projects_.end() && "Project already exists");
            projects_[id] = this;
        }

        static void ImportFrom(std::string const & filename);

        std::string const repo;
        std::string const user;

    private:

        static std::unordered_map<unsigned, Project *> projects_;
        
    }; // dejavu::Project


    class Path : public Object {
    public:

        Path(unsigned id, std::string const & path):
            Object(id),
            path(path) {
            assert(paths_.find(id) == paths_.end() && "Path already exists");
            paths_[id] = this;
        }

        static void ImportFrom(std::string const & filename);

        std::string const path;

    private:

        static std::unordered_map<unsigned, Path *> paths_;
        
        
    }; // dejavu::Path
    
} //namespace dejavu
