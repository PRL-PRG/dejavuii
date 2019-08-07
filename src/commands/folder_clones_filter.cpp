#include <unordered_map>
#include <vector>

#include "../loaders.h"
#include "../commands.h"


/*

peta@prl1e:~/devel/dejavuii/build$ time ./dejavu filter-folder-clones -d=/data/dejavu/no-npm
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading original candidates...
    14803308 originals loaded
Loading clone candidates...
    32853682 clone candidates loaded
    1353910 clone originals are clones themseves
    11810456 removed clone originals
Merging identical clones...
    187627 merges done
reindexing and writing clone originals...
    2805225 clone originals written
writing clone occurences...
    19689316 occurences written
    17514095 of which are complete clones
KTHXBYE!

real    1m28.149s
user    0m59.629s
sys     0m28.507s        

 */


/** Filters the folder clones obtained by previous phases.

    First loads the clone originals and the combs through the clone occurences keeping only occurences of valid clones. I.e. if the clone original is the sole clone occurence itself, then it is not really a clone.

    In the second stage, renumbers the clone originals and their occurences taking into account the missing ones.

    Since the input data is relatively small (tens of GBs), all is performed in memory. 
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
            
            /** True if the original of the clone is one of its occurences (in this case the occurence itself will not be reported in clone occurences, but this flag is set).
             */
            bool isCloneItself;

            CloneOriginal(unsigned id, SHA1Hash hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path):
                id(id),
                hash(hash),
                occurences(occurences),
                files(files),
                projectId(projectId),
                commitId(commitId),
                path(path),
                isCloneItself(false) {
            }

            friend std::ostream & operator << (std::ostream & s, CloneOriginal const & c) {
                s << c.id << "," << c.hash << "," << c.occurences << "," << c.files << "," << c.projectId << "," << c.commitId << "," << helpers::escapeQuotes(c.path) << "," << (c.isCloneItself ? "1" : "0");
                return s;
            }
        };

        class CloneOccurence {
        public:
            unsigned cloneId;
            unsigned projectId;
            unsigned commitId;
            std::string folder;
            unsigned files;

            CloneOccurence(unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & folder, unsigned files):
                cloneId(cloneId),
                projectId(projectId),
                commitId(commitId),
                folder(folder),
                files(files) {
            }
            friend std::ostream & operator << (std::ostream & s, CloneOccurence const & c) {
                s << c.cloneId << "," << c.projectId << "," << c.commitId << "," << helpers::escapeQuotes(c.folder) << "," << c.files;
                return s;
            }
        }; // Clone Occurence

        class FolderClonesFilterer {
        public:

            void loadData() {
                std::cerr << "Loading original candidates..." << std::endl;
                FolderCloneOriginalsCandidateLoader{DataDir.value() + "/folderCloneOriginalCandidates.csv", [this](unsigned id, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path){
                        if (id >= originals_.size())
                            originals_.resize(id + 1);
                        originals_[id] = new CloneOriginal(id, hash, occurences, files, projectId, commitId, path);
                    }};
                std::cerr << "    " << originals_.size() << " originals loaded" << std::endl;
                std::cerr << "Loading clone candidates..." << std::endl;
                unsigned total = 0;
                unsigned removed = 0;
                unsigned isClone = 0;
                FolderCloneCandidateLoader{[&, this](unsigned cloneId, unsigned projectId, unsigned commitId, std::string const & folder, unsigned files) {
                        if (cloneId >= originals_.size())
                            std::cout << cloneId << std::endl;
                        assert(cloneId < originals_.size());
                        CloneOriginal * original = originals_[cloneId];
                        assert(original != nullptr);
                        ++total;
                        if (original->projectId == projectId && original->commitId == commitId && original->path == folder) {
                            original->isCloneItself = true;
                            if (--original->occurences == 0) {
                                delete original;
                                originals_[cloneId] = nullptr;
                                ++removed;
                            } else {
                                ++isClone;
                            }
                        } else {
                            occurences_.push_back(new CloneOccurence(cloneId, projectId, commitId, folder, files));
                        }
                    }};
                std::cerr << "    " << total << " clone candidates loaded" << std::endl;
                std::cerr << "    " << isClone << " clone originals are clones themseves" << std::endl;
                std::cerr << "    " << removed << " removed clone originals" << std::endl;
            }

            /** It is theoretically possible that two clones are actually from the same source, just a different subset of files. This step makes sure that all such clones are joined into a single clone original.
             */
            void mergeIdenticalOriginals() {
                std::cerr << "Merging identical clones..." << std::endl;
                // commitId -> path -> original
                unsigned merges = 0;
                std::unordered_map<unsigned, std::unordered_map<std::string, CloneOriginal *>> x;
                for (CloneOriginal * & co : originals_) {
                    // if the clone was already deleted, ignore it
                    if (co == nullptr)
                        continue;
                    // otherwise see if we already have a clone for the commit & path
                    CloneOriginal * & existing = x[co->commitId][co->path];
                    if (existing == nullptr) {
                        existing = co;
                    } else {
                        assert(existing->projectId == co->projectId);
                        assert(existing->files == co->files);
                        existing->occurences += co->occurences;
                        co->files = 0; // to indicate that the clone does not exist
                        co->id = existing->id; // pointer to the first clone
                        ++merges;
                    }
                }
                std::cout << "    " << merges << " merges done" << std::endl;
            }

            void reindexAndOutput() {
                std::cout << "reindexing and writing clone originals..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/folderCloneOriginals.csv");
                    f << "cloneId,hash,occurences,files,projectId,commitId,path,isOriginalClone" << std::endl;
                    size_t id = 0;
                    for (CloneOriginal * co : originals_) {
                        if (co == nullptr)
                            continue;
                        if (co->files == 0)
                            continue;
                        // update the id
                        co->id = id++;
                        // and output
                        f << *co << std::endl;
                    }
                    std::cerr << "    " << id << " clone originals written" << std::endl;
                }
                std::cout << "writing clone occurences..." << std::endl;
                {
                    std::ofstream f(DataDir.value() + "/folderCloneOccurences.csv");
                    f << "cloneId,projectId,commitId,path,files" << std::endl;
                    size_t completeClones = 0;
                    for (CloneOccurence * cc : occurences_) {
                        CloneOriginal * co = originals_[cc->cloneId];
                        if (co->files == 0)
                            co = originals_[co->id];
                        assert(co->files != 0);
                        assert(co->files >= cc->files);
                        if (co->files == cc->files)
                            ++completeClones;
                        cc->cloneId = co->id;
                        f << *cc << std::endl;
                    }
                    std::cerr << "    " << occurences_.size() << " occurences written" << std::endl;
                    std::cerr << "    " << completeClones << " of which are complete clones" << std::endl;
                }
                
            }

        private:
            std::vector<CloneOriginal *> originals_;
            std::vector<CloneOccurence *> occurences_;

        }; // Filter

    }


    void FolderClonesClean(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        FolderClonesFilterer f;
        f.loadData();
        f.mergeIdenticalOriginals();
        f.reindexAndOutput();
    }

    
} // namespace dejavu
