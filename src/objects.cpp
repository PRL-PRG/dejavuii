#include "objects.h"

#include "helpers/csv-reader.h"

namespace dejavu {

    namespace {

        class CommitsLoader : public Commit::Reader {
        protected:
            void onRow(unsigned id, std::string const & hash, uint64_t time) override {
                new Commit(id, hash, time);
            }
        }; // CommitsLoader

        class ProjectsLoader : public Project::Reader {
        protected:
            void onRow(unsigned id, std::string const & user, std::string const & repo) override {
                new Project(id, repo, user);
            }
        }; // ProjectsLoader

        class PathsLoader : public Path::Reader {
        protected:
            void onRow(unsigned id, std::string const & path) override {
                new Path(id, path);
            }
        }; // PathsLoader

        class SnapshotsLoader : public Snapshot::Reader {
        protected:
            void onRow(unsigned id, std::string const & hash) override {
                new Snapshot(id, hash);
            }
        }; // SnapshotsLoader

    } //anonymous namespace

    std::unordered_map<unsigned, Commit *> Commit::commits_;
    std::unordered_map<unsigned, Project *> Project::projects_;
    std::unordered_map<unsigned, Path *> Path::paths_;
    std::unordered_map<unsigned, Snapshot *> Snapshot::snapshots_;


    void Commit::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        CommitsLoader l;
        size_t numRows = l.readFile(filename);
        std::cerr << "Total number of commits " << numRows << std::endl;
    }

    void Project::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        ProjectsLoader l;
        size_t numRows = l.readFile(filename);
        std::cerr << "Total number of projects " << numRows << std::endl;
    }

    void Path::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        PathsLoader l;
        size_t numRows = l.readFile(filename);
        std::cerr << "Total number of paths " << numRows << std::endl;
    }

    void Snapshot::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        SnapshotsLoader l;
        size_t numRows = l.readFile(filename);
        std::cerr << "Total number of snapshots " << numRows << std::endl;
    }

    void ImportFiles(std::string const & filename) {
        //std::cerr << "Importing from file " << filename << std::endl;
        //        size_t x = FilesLoader::LoadFile(filename);
        //std::cerr << "Total number of files " << x << std::endl;
        NOT_IMPLEMENTED;
    }

} // namespace dejavu