#include <unordered_map>
#include <vector>

#include "../loaders.h"
#include "../commands.h"


/**


 */
namespace dejavu {

    namespace {

        class CloneOriginal {
        public:
            unsigned id;
            unsigned numFiles;
            unsigned projectId;
            unsigned commitId;
            std::string rootDir;

            CloneOriginal(unsigned id, unsigned numFiles, unsigned projectId, unsigned commitId, std::string const & rootDir):
                id(id),
                numFiles(numFiles),
                projectId(projectId),
                commitId(commitId),
                rootDir(rootDir) {
            }
        };

        class Filter {
        public: 
            void initialize() {
                // now thge basic data has been loaded, load the clone originals
                std::cerr << "Loading clone originals..." << std::endl;
                FolderCloneOriginalsLoader{[this](unsigned id, unsigned numFiles, unsigned projectId, unsigned commitId, std::string const & rootDir) {
                        std::string cid = STR(projectId << "," << commitId << "," << rootDir);
                        auto i = uniqueOriginals_.find(cid);
                        if (i != uniqueOriginals_.end()) {
                            translation_[id] = i->second;
                            assert(numFiles == originals_[i->second]->numFiles);
                        } else {
                            CloneOriginal * ci = new CloneOriginal(originals_.size(), numFiles, projectId, commitId, rootDir);
                            translation_[id] = ci->id;
                            originals_.push_back(ci);
                            uniqueOriginals_[cid] = ci->id;
                        }
                    }};
                assert(originals_.size() == uniqueOriginals_.size());
                std::cerr << "    translations: " << translation_.size() << std::endl;
                std::cerr << "    originals:    " << originals_.size() << std::endl;
            }


        private:
            std::vector<CloneOriginal *> originals_;
            std::unordered_map<unsigned, unsigned> translation_;
            std::unordered_map<std::string, unsigned> uniqueOriginals_;


        }; // Filter

    }


    void FolderClonesFilter(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Filter f;
        f.initialize();
        
    }

    
} // namespace dejavu
