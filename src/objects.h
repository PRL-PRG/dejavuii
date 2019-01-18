#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "helpers/csv-reader.h"

namespace dejavu {

    class Object {
    public:
        unsigned const id;

    protected:
        Object(unsigned id):
            id(id) {
        }
    };

    class Project;
    class Path;
    
    class Commit : public Object {
    public:

        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename) {
                numRows_ = 0;
                parse(filename);
                onDone(numRows_);
                return numRows_;
            }
        protected:

            virtual void onRow(unsigned id, std::string const & hash, uint64_t time) = 0;

            virtual void onDone(size_t numRows) { }
            
            void row(std::vector<std::string> & row) override {
                assert(row.size() == 3 && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string hash = row[1];
                uint64_t t = std::stoull(row[2]);
                ++numRows_;
                onRow(id, hash, t);
            }

        private:
            
            size_t numRows_;
            
        }; // Commit::Reader

        
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

        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename) {
                numRows_ = 0;
                parse(filename);
                onDone(numRows_);
                return numRows_;
            }

        protected:

            virtual void onRow(unsigned id, std::string const & user, std::string const & repo) = 0;

            virtual void onDone(size_t numRows) { };

            void row(std::vector<std::string> & row) override {
                assert(row.size() == 3 && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string user = row[1];
                std::string repo = row[2];
                ++numRows_;
                onRow(id, user, repo);
            }

        private:

            size_t numRows_;
            
        }; // Project::Reader

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
        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename) {
                numRows_ = 0;
                parse(filename);
                onDone(numRows_);
                return numRows_;
            }

        protected:

            virtual void onRow(unsigned id, std::string const & path) = 0;

            virtual void onDone(size_t numRows) { };

            void row(std::vector<std::string> & row) override {
                assert(row.size() == 2 && "Invalid paths row length");
                unsigned id = std::stoul(row[0]);
                std::string path = row[1];
                ++numRows_;
                onRow(id, path);
            }

        private:

            size_t numRows_;
            
        }; // Path::Reader

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

        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename) {
                numRows_ = 0;
                parse(filename);
                onDone(numRows_);
                return numRows_;
            }

        protected:

            virtual void onRow(unsigned id, std::string const & hash) = 0;

            virtual void onDone(size_t numRows) { };

            void row(std::vector<std::string> & row) override {
                unsigned id = std::stoul(row[0]);
                ++numRows_;
                // id 0 == deleted, no hash
                if (id != 0) {
                    assert(row.size() == 2 && "Invalid snapshot row length");
                    std::string hash = row[1];
                    onRow(id, hash);
                } 
            }

        private:

            size_t numRows_;
            
        }; // Snapshot::Reader
        
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



    class Folder;

    class PathSegment {
    public:
        std::string const name;
        Folder const * parent;
    protected:
    }; // dejavu::PathSegment

    class File : public PathSegment {
        
    }; // dejavu File


    
    class Folder : public PathSegment {
    public:
    protected:
        std::unordered_map<std::string, PathSegment *> children_;
    }; // dejavu::Folder



    class FileRecord {
    public:
        /** Loads the files and builds the representation in memory so that it can be queried.
         */
        class Reader : public helpers::CSVReader {
        public:

            size_t readFile(std::string const & filename) {
                numRecords_ = 0;
                parse(filename);
                onDone(numRecords_);
                return numRecords_;
            }
            
        protected:

            virtual void onRow(unsigned projectId, unsigned pathId, unsigned snapshotId, unsigned commitId) = 0;
            virtual void onDone(size_t numRows) { }

            void row(std::vector<std::string> & row) override {
                assert(row.size() ==  4 && "Invalid file row length");
                unsigned projectId = std::stoul(row[0]);
                unsigned pathId = std::stoul(row[1]);
                unsigned snapshotId = std::stoul(row[2]);
                unsigned commitId = std::stoul(row[3]);
                ++numRecords_;
                onRow(projectId, pathId, snapshotId, commitId);
            }

        
            size_t numRecords_ = 0;
        }; // FileRecord

    private:
        
        FileRecord() = delete;
        
    }; 
    


    

    
    void ImportFiles(std::string const & filename);
    
} //namespace dejavu
