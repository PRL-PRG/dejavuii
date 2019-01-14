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


    class Project;
    class Path;
    
    class Commit : public Object {
    public:
        Commit(unsigned id, std::string const & hash, uint64_t time):
            Object(id),
            hash(hash),
            time(time) {
            assert(commits_.find(id) == commits_.end() && "Commit already exists");
            commits_[id] = this;
        }


        static Commit * Get(unsigned id) {
            auto i = commits_.find(id);
            assert(i != commits_.end() && "Unknown commit");
            return i->second;
        }
        
        static void ImportFrom(std::string const & filename);

        std::string const hash;
        uint64_t const time;

    private:

        /** All projects which contain the given commit.
         */
        std::unordered_map<unsigned, Project *> projects_;

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

        static Project * Get(unsigned id) {
            auto i = projects_.find(id);
            assert(i != projects_.end() && "Unknown project");
            return i->second;
        }
        
        static void ImportFrom(std::string const & filename);

        std::string const repo;
        std::string const user;

    private:

        /** All commits that belong to the project.
         */
        std::unordered_map<unsigned, Commit *> commits_;
        
        /** All paths that belong to the project.
         */
        std::unordered_map<unsigned, Path*> paths_;

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

        static Path * Get(unsigned id) {
            auto i = paths_.find(id);
            assert(i != paths_.end() && "Unknown path");
            return i->second;
        }

        static void ImportFrom(std::string const & filename);

        std::string const path;

    private:

        static std::unordered_map<unsigned, Path *> paths_;
        
    }; // dejavu::Path

    class Snapshot : public Object {
    public:
        Snapshot(unsigned id, std::string const & hash):
            Object(id),
            hash(hash) {
            assert(snapshots_.find(id) == snapshots_.end() && "Snapshot already exists");
            snapshots_[id] = this;
        }

        static Snapshot * Get(unsigned id) {
            if (id == 0)
                return nullptr; // deleted snapshot
            auto i = snapshots_.find(id);
            assert(i != snapshots_.end() && "Unknown snapshot");
            return i->second;
        }
        
        static void ImportFrom(std::string const & filename);

        std::string const hash;

    private:

        static std::unordered_map<unsigned, Snapshot *> snapshots_;
        
    }; // dejavu::Snapshot

    void ImportFiles(std::string const & filename);
    
} //namespace dejavu
