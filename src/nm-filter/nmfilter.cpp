
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <unordered_set>


#include "helpers/helpers.h"
#include "helpers/csv-reader.h"
#include "../settings.h"

#include "nmfilter.h"

namespace dejavu {

    class PathsParser : public helpers::CSVReader {
    public:
        static std::unordered_set<unsigned> GetValidPaths(std::string const & idir, std::string const & odir, std::ofstream & stats) {
            PathsParser p;
            p.nonNmPaths_.open(odir + "/paths.csv");
            p.parse(idir + "/paths.csv", false);
            std::cout << "node_modules paths: " << p.numNmPaths_ << std::endl;
            std::cout << "non node paths:     " << p.numNonNmPaths_ << std::endl;
            stats << "paths_node_modules," << p.numNmPaths_ << std::endl;
            stats << "paths_valid," << p.numNonNmPaths_ << std::endl;;
            return std::move(p.validPaths_);
        }
    protected:
        void row(std::vector<std::string> & row) override {
            if (row[1].find("node_modules") != std::string::npos) {
                ++numNmPaths_;
            } else {
                unsigned id = std::atoi(row[0].c_str());
                validPaths_.insert(id);
                nonNmPaths_ << row[0] << ",\"" << row[1] << "\"" << std::endl;
                ++numNonNmPaths_;
            }
        }

        std::unordered_set<unsigned> validPaths_;
        std::ofstream nonNmPaths_;

        unsigned numNmPaths_ = 0;
        unsigned numNonNmPaths_ = 0;
    };

    class FilesParser : public helpers::CSVReader {
    public:
        struct ValidIds {
            std::unordered_set<unsigned> commits;
            std::unordered_set<unsigned> projects;
            std::unordered_set<unsigned> fileHashes;
        };

        static ValidIds GetValidIds(std::string const & idir, std::string const & odir, std::ofstream & stats, std::unordered_set<unsigned> const & validPaths) {
            FilesParser p(validPaths);
            p.noNmFiles_.open(odir + "/files.csv");
            p.parse(idir + "/files.csv", false);
            std::cout << "node_modules files: " << p.numNm_ << std::endl;
            std::cout << "non node files:     " << p.numNoNm_ << std::endl;
            stats << "files_node_modules," << p.numNm_ << std::endl;
            stats << "files_valid," << p.numNoNm_ << std::endl;;
            return std::move(p.valid_);
        }

    protected:

        FilesParser(std::unordered_set<unsigned> const & validPaths):
            validPaths_(validPaths) {
        }

        void row(std::vector<std::string> & row) override {
            unsigned pathId = std::atoi(row[1].c_str());
            if (validPaths_.find(pathId) != validPaths_.end()) {
                unsigned commitId = std::atoi(row[3].c_str());
                unsigned projectId = std::atoi(row[0].c_str());
                unsigned fileHash = std::atoi(row[2].c_str());
                noNmFiles_ << row[0] << "," << row[1] << "," << row[2] << "," << row[3] << std::endl;
                valid_.commits.insert(commitId);
                valid_.projects.insert(projectId);
                valid_.fileHashes.insert(fileHash);
                ++numNoNm_;
            } else {
                ++numNm_;
            }
        }

        std::unordered_set<unsigned> const & validPaths_;
        std::ofstream noNmFiles_;
        unsigned numNm_ = 0;
        unsigned numNoNm_ = 0;

        ValidIds valid_;
    };

    class CommitsParser : public helpers::CSVReader {
    public:
        static void Validate(std::string const & idir, std::string const & odir, std::ofstream & stats, std::unordered_set<unsigned> const & validIds) {
            CommitsParser p(validIds);
            p.noNmCommits_.open(odir + "/commits.csv");
            p.parse(idir + "/commits.csv", false);
            std::cout << "node_modules only commits: " << p.numNm_ << std::endl;
            std::cout << "regular commits:           " << p.numNoNm_ << std::endl;
            stats << "commits_node_modules_only," << p.numNm_ << std::endl;
            stats << "commits_valid," << p.numNoNm_ << std::endl;;
        }
    
    private:

        CommitsParser(std::unordered_set<unsigned> const & valid):
            valid_(valid) {
        }
        void row(std::vector<std::string> & row) override {
            unsigned id = std::atoi(row[0].c_str());
            if (valid_.find(id) != valid_.end()) {
                noNmCommits_ << row[0] << "," << row[1] << "," << row[2] << std::endl;
                ++numNoNm_;
            } else {
                ++numNm_;
            }
        }

        std::unordered_set<unsigned> const & valid_;
        std::ofstream noNmCommits_;
        unsigned numNm_ = 0;
        unsigned numNoNm_ = 0;
    };

    class ProjectsParser : public helpers::CSVReader {
    public:
        static void Validate(std::string const & idir, std::string const & odir, std::ofstream & stats, std::unordered_set<unsigned> const & validIds) {
            ProjectsParser p(validIds);
            p.noNmProjects_.open(odir + "/projects.csv");
            p.parse(idir + "/projects.csv", false);
            std::cout << "node_modules only projects: " << p.numNm_ << std::endl;
            std::cout << "regular projects:           " << p.numNoNm_ << std::endl;
            stats << "projects_node_modules_only," << p.numNm_ << std::endl;
            stats << "projects_valid," << p.numNoNm_ << std::endl;;
        }
    
    private:

        ProjectsParser(std::unordered_set<unsigned> const & valid):
            valid_(valid) {
        }
        void row(std::vector<std::string> & row) override {
            unsigned id = std::atoi(row[0].c_str());
            if (valid_.find(id) != valid_.end()) {
                noNmProjects_ << row[0] << "," << row[1] << std::endl;
                ++numNoNm_;
            } else {
                ++numNm_;
            }
        }

        std::unordered_set<unsigned> const & valid_;
        std::ofstream noNmProjects_;
        unsigned numNm_ = 0;
        unsigned numNoNm_ = 0;
    };

    class FileHashParser : public helpers::CSVReader {
    public:
        static void Validate(std::string const & idir, std::string const & odir, std::ofstream & stats, std::unordered_set<unsigned> const & validIds) {
            FileHashParser p(validIds);
            p.noNmFileHashes_.open(odir + "/fileHashes.csv");
            p.parse(idir + "/fileHashes.csv", false);
            std::cout << "node_modules only fileHashes: " << p.numNm_ << std::endl;
            std::cout << "regular fileHashes:           " << p.numNoNm_ << std::endl;
            stats << "fileHashes_node_modules_only," << p.numNm_ << std::endl;
            stats << "fileHashes_valid," << p.numNoNm_ << std::endl;;
        }
    
    private:

        FileHashParser(std::unordered_set<unsigned> const & valid):
            valid_(valid) {
        }
        void row(std::vector<std::string> & row) override {
            unsigned id = std::atoi(row[0].c_str());
            if (valid_.find(id) != valid_.end()) {
                if (row.size() == 1)
                    noNmFileHashes_ << row[0] << "," << std::endl;
                else
                    noNmFileHashes_ << row[0] << "," << row[1] << std::endl;
                ++numNoNm_;
            } else {
                ++numNm_;
            }
        }

        std::unordered_set<unsigned> const & valid_;
        std::ofstream noNmFileHashes_;
        unsigned numNm_ = 0;
        unsigned numNoNm_ = 0;
    };

    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", {"-if"}, false);
        helpers::Option<std::string> OutputDir("outputDir", "/processed_no_node", {"-o"}, false);
    }
    
    void RemoveNodeModules(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        if (InputDir.value() == OutputDir.value())
            throw std::runtime_error("Input and output directories are the same. This would not work");
        std::string idir = DataRoot.value() + InputDir.value();
        std::string odir = DataRoot.value() + OutputDir.value();
        helpers::EnsurePath(odir);
        std::ofstream stats(odir + "/node_removal_done.csv", std::ios::out);
        stats << "category,size" << std::endl;
        // determine which paths are valid (i.e. those that do not have node_module prefix)
        auto validPaths = PathsParser::GetValidPaths(idir, odir, stats);
        // now we can look at files and we can keep track of used commit, project and file hashes ids used 
        FilesParser::ValidIds validIds = FilesParser::GetValidIds(idir, odir, stats, validPaths);
        // given this, we just filter the remaining commits, projects and file hashes
        CommitsParser::Validate(idir, odir, stats, validIds.commits);
        ProjectsParser::Validate(idir, odir, stats, validIds.projects);
        FileHashParser::Validate(idir, odir, stats, validIds.fileHashes);
    }
}

