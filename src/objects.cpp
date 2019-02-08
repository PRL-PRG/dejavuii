#include "objects.h"

#include "helpers/csv-reader.h"

namespace dejavu {

    namespace {

        class HashLoader : public Hash::Reader {
        protected:
            void onRow(std::string const & hash) override {
                //new dejavu::Hash(hash);
                Hash::Register(hash);
            }
        }; // HashLoader

        class CommitsLoader : public Commit::Reader {
        protected:
            void onRow(unsigned id, std::string const & hash, uint64_t time) override {
                new Commit(id, hash, time);
            }

            virtual void onRow(unsigned id, std::string const & hash, uint64_t time, unsigned numProjects, unsigned originalProject) {
                new Commit(id, hash, time, numProjects, originalProject);
            }
            
        }; // CommitsLoader

        class ProjectsLoader : public Project::Reader {
        protected:
            void onRow(unsigned id, std::string const & user, std::string const & repo) override {
                new Project(id, repo, user);
            }
            
            void onRow(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt, int fork, unsigned committers, unsigned authors, unsigned watchers) override {
                new Project(id, repo, user, createdAt, fork, committers, authors, watchers);
            }
        }; // ProjectsLoader

        class PathsLoader : public Path::Reader {
        protected:
            void onRow(unsigned id, std::string const & path) override {
                new Path(id, path);
            }
        }; // PathsLoader

        class FileHashesLoader : public FileHash::Reader {
        protected:
            void onRow(unsigned id, std::string const & hash) override {
                new FileHash(id, hash);
            }
            void onRow(unsigned id, std::string const & hash, unsigned creatorCommit, unsigned occurences, unsigned paths, unsigned commits, unsigned projects) override {
                new FileHash(id, hash, creatorCommit, occurences, paths, commits, projects);
            }
        }; // SnapshotsLoader

    } //anonymous namespace

    std::unordered_map<unsigned, Commit *> Commit::commits_;
    std::unordered_map<unsigned, Project *> Project::projects_;
    std::unordered_map<unsigned, Path *> Path::paths_;
    std::set<std::string> Hash::hashes_;
    std::unordered_map<unsigned, FileHash *> FileHash::fileHashes_;

    void Hash::ImportFrom(std::string const & filename, bool headers, unsigned int column) {
        std::cerr << "Importing from file " << filename << std::endl;
        HashLoader l;
        size_t numRows = l.readFile(filename, headers, column);
        std::cerr << "Total number of hashes " << numRows << std::endl;
    }

    void Commit::ImportFrom(std::string const & filename, bool headers) {
        std::cerr << "Importing from file " << filename << std::endl;
        CommitsLoader l;
        size_t numRows = l.readFile(filename, headers);
        std::cerr << "Total number of commits " << numRows << std::endl;
    }

    void Project::ImportFrom(std::string const & filename, bool headers) {
        std::cerr << "Importing from file " << filename << std::endl;
        ProjectsLoader l;
        size_t numRows = l.readFile(filename, headers);
        std::cerr << "Total number of projects " << numRows << std::endl;
    }

    void Path::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        PathsLoader l;
        size_t numRows = l.readFile(filename);
        std::cerr << "Total number of paths " << numRows << std::endl;
    }

    void FileHash::ImportFrom(std::string const & filename, bool headers) {
        std::cerr << "Importing from file " << filename << std::endl;
        FileHashesLoader l;
        size_t numRows = l.readFile(filename, headers);
        std::cerr << "Total number of snapshots " << numRows << std::endl;
    }

    void ImportFiles(std::string const & filename) {
        //std::cerr << "Importing from file " << filename << std::endl;
        //        size_t x = FilesLoader::LoadFile(filename);
        //std::cerr << "Total number of files " << x << std::endl;
        NOT_IMPLEMENTED;
    }

} // namespace dejavu
