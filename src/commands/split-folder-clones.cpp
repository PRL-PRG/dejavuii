#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <openssl/sha.h>
#include <fstream>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


/** Splits the folder clones into different categories so that they can be removed in different stages.

    Forks = project root is copied to other project root
    Submodules = project root is copied to other project folder (not root)
    Folders = non root folder is copied to other project
 */

namespace dejavu {

    namespace {

        class CloneOriginal {
        public:
            unsigned id;
            SHA1Hash hash;
            unsigned occurences;
            unsigned files;
            unsigned projectId;
            unsigned commitId;
            std::string path;
            bool isOriginal;

            
        }; 

        class Splitter {
        public:
            void loadDataAndSplit() {
                std::cerr << "Loading clone originals..." << std::endl;
                FolderCloneOriginalsLoader([this](unsigned cloneId, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path, bool isOriginal) {
                        originals_.insert(std::make_pair(cloneId, new CloneOriginal{cloneId, hash, occurences, files, projectId, commitId, path, isOriginal}));
                    });
                std::cerr << "    " << originals_.size() << " originals loaded" << std::endl;
                std::cerr << "Loading clone occurences..." << std::endl;
                {
                    size_t total = 0;
                    size_t intraproject = 0;
                    size_t forks = 0;
                    size_t submodules = 0;
                    size_t folders = 0;
                    std::ofstream forkClones(DataDir.value() + "/forkClones.csv");
                    std::ofstream submoduleClones(DataDir.value() + "/submoduleClones.csv");
                    std::ofstream folderClones(DataDir.value() + "/folderOnlyClones.csv");
                    FolderCloneOccurencesLoader([&, this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & folder, unsigned files) {
                            ++total;
                            CloneOriginal * co = originals_[cloneId];
                            if (co->projectId == projectId) {
                                ++intraproject;
                                assert(co->occurences > 0);
                                --co->occurences;
                            } else {
                                std::ofstream * x;
                                if (co->path.empty()) {
                                    if (folder.empty()) {
                                        ++forks;
                                        x = &forkClones;
                                    } else {
                                        ++submodules;
                                        x = &submoduleClones;
                                    }
                                } else {
                                    ++folders;
                                    x = &folderClones;
                                }
                                (*x) << cloneId << "," << projectId << "," << commitId << "," << helpers::escapeQuotes(folder) << "," << files << std::endl;
                            }
                    });
                    std::cerr << "    " << total << " total clone occurences read" << std::endl;
                    std::cerr << "    " << intraproject << " intraproject clones" << std::endl;
                    std::cerr << "    " << forks << " project to project (fork) clones" << std::endl;
                    std::cerr << "    " << submodules << " project to folder (submodule) clones" << std::endl;
                    std::cerr << "    " << folders << " folder clones" << std::endl;
                }
                // and now, we have to output the intraproject originals
                {
                    std::cerr << "Writing surviving originals..." << std::endl;
                    std::ofstream origs(DataDir.value()+"/interProjectFolderCloneOriginals.csv");
                    size_t total = 0;
                    for (auto i : originals_) {
                        CloneOriginal * co = i.second;
                        if (co->occurences > 0) {
                            origs << co->id << ","
                                  << co->hash << ","
                                  << co->occurences << ","
                                  << co->files << ","
                                  << co->projectId << ","
                                  << co->commitId << ","
                                  << helpers::escapeQuotes(co->path) << ","
                                  << (co->isOriginal ? "1" : "0") << std::endl;
                            ++total;
                        }
                    }
                    std::cerr << "    " << total << " clone originals survived" << std::endl;
                }
            }

            unsigned id;
            SHA1Hash hash;
            unsigned occurences;
            unsigned files;
            unsigned projectId;
            unsigned commitId;
            std::string path;
            bool isOriginal;

            
        private:
            std::unordered_map<unsigned, CloneOriginal*> originals_;
            
        };


        
    } // anonymous namespace


    void SplitFolderClones(int argc, char *argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Splitter s;
        s.loadDataAndSplit();

        
    }


} // namespace dejavu
