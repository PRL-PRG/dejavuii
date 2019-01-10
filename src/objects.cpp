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
                assert(row.size() == 2 && "Invalid commit row length");
                unsigned id = std::stoul(row[0]);
                std::string path = row[1];
                new Path(id, path);
            }
        }; // PathsLoader


        
        
    } //anonymous namespace

    std::unordered_map<unsigned, Commit *> Commit::commits_;
    std::unordered_map<unsigned, Project *> Project::projects_;
    std::unordered_map<unsigned, Path *> Path::paths_;


    void Commit::ImportFrom(std::string const & filename) {
        std::cout << "Importing from file " << filename << std::endl;
        CommitsLoader::LoadFile(filename);
        std::cout << "Total number of commits " << commits_.size() << std::endl;
    }

    void Project::ImportFrom(std::string const & filename) {
        std::cout << "Importing from file " << filename << std::endl;
        ProjectsLoader::LoadFile(filename);
        std::cout << "Total number of projects " << projects_.size() << std::endl;
    }

    void Path::ImportFrom(std::string const & filename) {
        std::cout << "Importing from file " << filename << std::endl;
        PathsLoader::LoadFile(filename);
        std::cout << "Total number of paths " << paths_.size() << std::endl;
        
    }
    
} // namespace dejavu
