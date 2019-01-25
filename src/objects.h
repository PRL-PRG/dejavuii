#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_map>

#include "helpers/csv-reader.h"
#include "helpers/strings.h"

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
            size_t readFile(std::string const & filename, bool headers) {
                numRows_ = 0;
                parse(filename, headers);
                onDone(numRows_);
                return numRows_;
            }
        protected:

            virtual void onRow(unsigned id, std::string const & hash, uint64_t time) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onRow(unsigned id, std::string const & hash, uint64_t time, unsigned numProjects, unsigned originalProject) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onDone(size_t numRows) { }
            
            void row(std::vector<std::string> & row) override {
                assert((row.size() == 3 || row.size() == 5) && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string hash = row[1];
                uint64_t t = std::stoull(row[2]);
                if (row.size() == 5) {
                    unsigned numProjects = std::stoul(row[3]);
                    unsigned originalProject = std::stoul(row[4]);
                    ++numRows_;
                    onRow(id, hash, t, numProjects, originalProject);
                } else {
                    ++numRows_;
                    onRow(id, hash, t);
                }
            }

        private:
            
            size_t numRows_;
            
        }; // Commit::Reader

        Commit(unsigned id, std::string const & hash, uint64_t time):
            Object(id),
            hash(hash),
            time(time),
            numProjects(0),
            originalProject(0) {
            assert(commits_.find(id) == commits_.end() && "Commit already exists");
            commits_[id] = this;
        }

        Commit(unsigned id, std::string const & hash, uint64_t time, unsigned numProjects, unsigned originalProject):
            Object(id),
            hash(hash),
            time(time),
            numProjects(numProjects),
            originalProject(originalProject) {
            assert(commits_.find(id) == commits_.end() && "Commit already exists");
            commits_[id] = this;
        }


        static Commit * Get(unsigned id) {
            auto i = commits_.find(id);
            assert(i != commits_.end() && "Unknown commit");
            return i->second;
        }

        static std::unordered_map<unsigned, Commit *> const & AllCommits() {
            return commits_;
        }
        
        static void ImportFrom(std::string const & filename, bool headers);

        static void SaveAll(std::string const & filename) {
            std::ofstream s(filename);
            if (! s.good())
                ERROR("Unable to open file " << filename << " for writing");
            s << "id,hash,time,numProjects,originalProjectId" << std::endl;    
            for (auto i : commits_)
                s << * (i.second) << std::endl;
        }

        
        std::string const hash;
        
        uint64_t const time;

        unsigned numProjects;
        unsigned originalProject;

    private:
        friend std::ostream & operator << (std::ostream & s, Commit const & c) {
            s << c.id << "," << c.hash << "," << c.time << "," << c.numProjects << "," << c.originalProject;
            return s;
        }

        /** All projects which contain the given commit.
         */
        std::unordered_map<unsigned, Project *> projects_;

        static std::unordered_map<unsigned, Commit *> commits_;
        
    }; // dejavu::Commit

    /**

     */
    class Project : public Object {
    public:

        static constexpr int NO_FORK = -1;
        static constexpr int UNKNOWN_FORK = -2;

        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename, bool headers) {
                numRows_ = 0;
                parse(filename, headers);
                onDone(numRows_);
                return numRows_;
            }

        protected:

            virtual void onRow(unsigned id, std::string const & user, std::string const & repo) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onRow(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt, int fork, unsigned committers, unsigned authors, unsigned watchers) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onDone(size_t numRows) { };

            void row(std::vector<std::string> & row) override {
                assert((row.size() == 3 || row.size() == 8) && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string user = row[1];
                std::string repo = row[2];
                if (row.size() == 8) {
                    uint64_t createdAt = std::stoull(row[3]);
                    int fork = std::stoi(row[4]);
                    unsigned committers = std::stoul(row[5]);
                    unsigned authors = std::stoul(row[6]);
                    unsigned watchers = std::stoul(row[7]);
                    ++numRows_;
                    onRow(id, user, repo, createdAt, fork, committers, authors, watchers);
                } else {
                ++numRows_;
                onRow(id, user, repo);
                }
            }

        private:

            size_t numRows_;
            
        }; // Project::Reader

        Project(unsigned id, std::string const & repo, std::string const & user):
            Object(id),
            repo(repo),
            user(user),
            createdAt(0),
            fork(NO_FORK),
            committers(0),
            authors(0),
            watchers(0) {
            assert(projects_.find(id) == projects_.end() && "Project already exists");
            projects_[id] = this;
        }

        Project(unsigned id, std::string const & repo, std::string const & user, uint64_t createdAt, int fork, unsigned committers, unsigned authors, unsigned watchers):
            Object(id),
            repo(repo),
            user(user),
            createdAt(createdAt),
            fork(fork),
            committers(committers),
            authors(authors),
            watchers(watchers) {
            assert(projects_.find(id) == projects_.end() && "Project already exists");
            projects_[id] = this;
        }

        static Project * Get(unsigned id) {
            auto i = projects_.find(id);
            assert(i != projects_.end() && "Unknown project");
            return i->second;
        }

        static std::unordered_map<unsigned, Project *> const & AllProjects() {
            return projects_;
        }

        static void SaveAll(std::string const & filename) {
            std::ofstream s(filename);
            if (! s.good())
                ERROR("Unable to open file " << filename << " for writing");
            s << "pid,user,repo,createdAt,fork,committers,authors,watchers" << std::endl;    
            for (auto i : projects_)
                s << * (i.second) << std::endl;
        }
        
        static void ImportFrom(std::string const & filename, bool headers);

        std::string const repo;
        std::string const user;
        uint64_t createdAt;
        int fork;
        unsigned committers;
        unsigned authors;
        unsigned watchers;

    private:

        friend std::ostream & operator << (std::ostream & s, Project const & p) {
            s << p.id << "," <<
                helpers::escapeQuotes(p.user) << "," <<
                helpers::escapeQuotes(p.repo) << "," <<
                p.createdAt << ","<<
                p.fork << "," <<
                p.committers << "," <<
                p.authors << "," <<
                p.watchers;
            return s;
        }

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
                parse(filename, false);
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

        static constexpr unsigned DELETED = 0;

        class Reader : public helpers::CSVReader {
        public:
            size_t readFile(std::string const & filename, bool headers) {
                numRows_ = 0;
                parse(filename, headers);
                onDone(numRows_);
                return numRows_;
            }

        protected:

            virtual void onRow(unsigned id, std::string const & hash) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onRow(unsigned id, std::string const & hash, unsigned creatorCommit, unsigned occurences, unsigned paths, unsigned commits, unsigned projects) {
                assert(false && "onRow method for used data layout not implemented");
            }

            virtual void onDone(size_t numRows) { };

            void row(std::vector<std::string> & row) override {
                unsigned id = std::stoul(row[0]);
                // id 0 == deleted, no hash
                if (id != 0) {
                    assert((row.size() == 2 || row.size() == 7) && "Invalid row length, unrecognized format");
                    std::string hash = row[1];
                    if (row.size() == 7) {
                        unsigned creatorCommit = std::stoul(row[2]);
                        unsigned occurences = std::stoul(row[3]);
                        unsigned paths = std::stoul(row[4]);
                        unsigned commits = std::stoul(row[5]);
                        unsigned projects = std::stoul(row[6]);
                        ++numRows_;
                        onRow(id, hash, creatorCommit, occurences, paths, commits, projects);
                    } else {
                        ++numRows_;
                        onRow(id, hash);
                    }
                } 
            }

        private:

            size_t numRows_;
            
        }; // Snapshot::Reader
        
        Snapshot(unsigned id, std::string const & hash):
            Object(id),
            hash(hash),
            creatorCommit(0),
            occurences(0),
            paths(0),
            commits(0),
            projects(0) {
            assert(snapshots_.find(id) == snapshots_.end() && "Snapshot already exists");
            snapshots_[id] = this;
        }
        Snapshot(unsigned id, std::string const & hash, unsigned creatorCommit, unsigned occurences, unsigned paths, unsigned commits, unsigned projects):
            Object(id),
            hash(hash),
            creatorCommit(creatorCommit),
            occurences(occurences),
            paths(paths),
            commits(commits),
            projects(projects) {
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

        static std::unordered_map<unsigned, Snapshot *> const & AllSnapshots() {
            return snapshots_;
        }
        
        static void ImportFrom(std::string const & filename, bool headers);

        static void SaveAll(std::string const & filename) {
            std::ofstream s(filename);
            if (! s.good())
                ERROR("Unable to open file " << filename << " for writing");
            s << "id,hash,creatorCommit,occurences,paths,commits,projects" << std::endl;    
            for (auto i : snapshots_)
                s << * (i.second) << std::endl;
        }
        
        std::string const hash;

        unsigned creatorCommit;
        unsigned occurences;
        unsigned paths;
        unsigned commits;
        unsigned projects;

    private:

        friend std::ostream & operator << (std::ostream & o, Snapshot const & s) {
            o << s.id << "," << s.hash << "," << s.creatorCommit << "," << s.occurences << "," << s.paths << "," << s.commits << "," << s.projects << std::endl;
            return o;
        }

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
                parse(filename, false);
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

namespace std {

    template<>
    class less<dejavu::Commit*> {
    public:
        bool operator () (dejavu::Commit * first, dejavu::Commit * second) {
            assert(first != nullptr && second != nullptr && "We don't expect null commit");
            return first->time < second->time;
        }
    }; // std::less<dejavu::Commit *>
    
} // namespace std
