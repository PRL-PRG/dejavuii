/* Because Java is sooooo memory heavyyyy, I am writing this in good ol' C++.
 */

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <dirent.h>

#include "helpers/csv-reader.h"
#include "helpers/strings.h"

#include "../settings.h"
#include "join.h"

namespace dejavu {

    namespace {

        class ChunksParser : public helpers::CSVReader {
        public:

            unsigned projectToId(std::string const & username, std::string const & project) {
                std::string fullName = username + "/" + project;
                auto i = projects.find(fullName);
                if (i != projects.end())
                    return i->second;
                unsigned id = projects.size();
                projects[fullName] = id;
                oProjects << id << ",\"" << username << "\",\"" << project << "\"" << std::endl;
                return id;
            }

            unsigned pathToId(std::string const & path) {
                auto i = paths.find(path);
                if (i != paths.end())
                    return i->second;
                unsigned id = paths.size();
                paths[path] = id;
                oPaths << id << ",\"" << helpers::escapeQuotes(path) << "\"" << std::endl;
                return id;
            }

            unsigned fileHashToId(std::string const & hash) {
                auto i = fileHashes.find(hash);
                if (i != fileHashes.end())
                    return i->second;
                unsigned id = fileHashes.size();
                fileHashes[hash] = id;
                oFileHashes << id << "," << hash << std::endl;
                return id;
            }

            unsigned commitToId(std::string const & hash, std::string const & time) {
                auto i = commits.find(hash);
                if (i != commits.end())
                    return i->second;
                unsigned id = commits.size();
                commits[hash] = id;
                oCommits << id << "," << hash << "," << time << std::endl;
                return id;
            }
    
            void row(std::vector<std::string> & row) override {
                //        std::cout << "." << std::flush;
                while (row.size() < 6) {
                    if (eof()) {
                        std::cout << row.size() << std::endl;
                        std::cout << row[0] << std::endl;
                        throw std::runtime_error("Incomplete row in CSV file");
                    }
                    append();
                }
                if (row.size() > 6) {
                    for (size_t i = 0, e = row.size(); i != e; ++i) {
                        std::cout << row[i] << std::endl;
                    }
                    throw std::runtime_error("Line too long");
                }
                unsigned projectId = projectToId(row[0], row[1]);
                unsigned pathId = pathToId(row[2]);
                unsigned fileHashId = fileHashToId(row[3]);
                unsigned commitId =commitToId(row[5], row[4]);
                oFiles << projectId << "," << pathId << "," << fileHashId << "," << commitId << std::endl;
                if (fileHashId == 0)
                    ++empty;
                ++numRows;
            }

            void parseDirectory(std::string const & path, std::string const & outputPath) {
                // make sure we have the output directory
                helpers::EnsurePath(outputPath);
                oProjects.open(outputPath + "/projects.csv");
                oPaths.open(outputPath + "/paths.csv");
                oFileHashes.open(outputPath + "/fileHashes.csv");
                oCommits.open(outputPath + "/commits.csv");
                oFiles.open(outputPath + "/files.csv");
                // add empty file hash
                fileHashToId("");
                DIR * d;
                dirent * dirp;
                if ((d = opendir(path.c_str())) == nullptr)
                    throw std::runtime_error(STR("Unable to open directory " + path));
                while ((dirp = readdir(d)) != nullptr) {
                    std::string fname = dirp->d_name;
                    if (fname == "." || fname == "..")
                        continue;
                    std::cout << fname << std::endl;
                    parse(path + "/" + fname, false);
                    std::cout << "    files       " << numRows << std::endl;
                    std::cout << "    deletions   " << empty << std::endl;
                    std::cout << "    projects    " << projects.size() << std::endl;
                    std::cout << "    paths       " << paths.size() << std::endl;
                    std::cout << "    file hashes " << fileHashes.size() << std::endl;
                    std::cout << "    commits     " << commits.size() << std::endl;
                }
                closedir(d);

                oProjects.close();
                oPaths.close();
                oFileHashes.close();
                oCommits.close();
                oFiles.close();

                std::ofstream done(outputPath + "/scala_join_done.csv", std::ios::out);
                done << "category,size" << std::endl;
                done << "files," << numRows << std::endl;
                done << "deletions," << empty << std::endl;
                done << "projects," << projects.size() << std::endl;
                done << "paths," << paths.size() << std::endl;
                done << "fileHashes," << fileHashes.size() << std::endl;
                done << "commits," << commits.size() << std::endl;
                done.close();
        
            }
            std::unordered_map<std::string, unsigned> projects;
            std::unordered_map<std::string, unsigned> paths;
            std::unordered_map<std::string, unsigned> fileHashes;
            std::unordered_map<std::string, unsigned> commits;

            std::ofstream oProjects;
            std::ofstream oPaths;
            std::ofstream oFileHashes;
            std::ofstream oCommits;
            std::ofstream oFiles;

            size_t numRows = 0;
            size_t empty = 0;

        };

        
    } // anonymous namespace



    namespace {
        helpers::Option<std::string> ChunksDir("chunksDir", "/original data from shabbir", false);
        helpers::Option<std::string> OutputDir("outputDir", "/processed", {"-o"}, false);
    }

    void JoinScalaChunks(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(ChunksDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        // do the work now
        ChunksParser p;
        p.parseDirectory(DataRoot.value() + ChunksDir.value(), DataRoot.value() + OutputDir.value());
    }


    
}; // namespace dejavu
