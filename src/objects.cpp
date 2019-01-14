#include "objects.h"

#include "helpers/csv-reader.h"

namespace dejavu {

    namespace {

        class CommitsLoader : public helpers::CSVReader {
        public:
            static void LoadFile(std::string const & filename) {
                CommitsLoader l;
                l.parse(filename);
            }        
            
        protected:
            void row(std::vector<std::string> & row) override {
                assert(row.size() == 3 && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string hash = row[1];
                uint64_t t = std::stoull(row[2]);
                new Commit(id, hash, t);
            }
        }; // CommitsLoader

        class ProjectsLoader : public helpers::CSVReader {
        public:
            static void LoadFile(std::string const & filename) {
                ProjectsLoader l;
                l.parse(filename);
            }        
            
        protected:
            void row(std::vector<std::string> & row) override {
                assert(row.size() == 3 && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string user = row[1];
                std::string repo = row[2];
                new Project(id, repo, user);
            }
        }; // ProjectsLoader

        class PathsLoader : public helpers::CSVReader {
        public:
            static void LoadFile(std::string const & filename) {
                PathsLoader l;
                l.parse(filename);
            }        
            
        protected:
            void row(std::vector<std::string> & row) override {
                if (row.size() != 2) {
                    for (auto i : row) std::cout << i << std::endl;
                }
                assert(row.size() == 2 && "Invalid paths row length");
                unsigned id = std::stoul(row[0]);
                std::string path = row[1];
                new Path(id, path);
            }
        }; // PathsLoader

        class SnapshotsLoader : public helpers::CSVReader {
        public:
            static void LoadFile(std::string const & filename) {
                SnapshotsLoader l;
                l.parse(filename);
            }        
            
        protected:
            void row(std::vector<std::string> & row) override {
                assert(row.size() > 0 && "Invalid snapshot row length");
                unsigned id = std::stoul(row[0]);
                // id 0 == deleted, no hash
                if (id != 0) {
                    assert(row.size() == 2 && "Invalid snapshot row length");
                    std::string hash = row[1];
                    new Snapshot(id, hash);
                }
            }
        }; // SnapshotsLoader

        /** Loads the files and builds the representation in memory so that it can be queried.
         */
        class FilesLoader : public helpers::CSVReader {
        public:
            static size_t LoadFile(std::string const & filename) {
                FilesLoader l;
                l.parse(filename);
                return l.numRecords_;
            }        
            
        protected:
            void row(std::vector<std::string> & row) override {
                assert(row.size() ==  4 && "Invalid file row length");
                Project * project = Project::Get(std::stoul(row[0]));
                Path * path = Path::Get(std::stoul(row[1]));
                Snapshot * snapshot = Snapshot::Get(std::stoul(row[2]));
                Commit * commit = Commit::Get(std::stoul(row[3]));
                ++numRecords_;
            }

            size_t numRecords_ = 0;
        }; // FilesLoader


        
        
    } //anonymous namespace

    std::unordered_map<unsigned, Commit *> Commit::commits_;
    std::unordered_map<unsigned, Project *> Project::projects_;
    std::unordered_map<unsigned, Path *> Path::paths_;
    std::unordered_map<unsigned, Snapshot *> Snapshot::snapshots_;


    void Commit::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        CommitsLoader::LoadFile(filename);
        std::cerr << "Total number of commits " << commits_.size() << std::endl;
    }

    void Project::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        ProjectsLoader::LoadFile(filename);
        std::cerr << "Total number of projects " << projects_.size() << std::endl;
    }

    void Path::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        PathsLoader::LoadFile(filename);
        std::cerr << "Total number of paths " << paths_.size() << std::endl;
    }

    void Snapshot::ImportFrom(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        SnapshotsLoader::LoadFile(filename);
        std::cerr << "Total number of snapshots " << snapshots_.size() << std::endl;
    }

    void ImportFiles(std::string const & filename) {
        std::cerr << "Importing from file " << filename << std::endl;
        size_t x = FilesLoader::LoadFile(filename);
        std::cerr << "Total number of files " << x << std::endl;
    }

} // namespace dejavu
