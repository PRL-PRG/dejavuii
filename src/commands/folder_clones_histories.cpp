#include <unordered_map>
#include <vector>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


/**


 */
namespace dejavu {

    namespace {

        class Commit {
        public:
            size_t id;
            uint64_t time;
            unsigned numParents;
            std::unordered_map<unsigned, unsigned> changes;
            std::vector<Commit *> children;

            // implementation for the commit iterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }

            unsigned numParentCommits() const {
                return numParents;
            }
            
        };

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;
            
        };

        /** Information about a clone.
         */
        class Clone {
        public:
            size_t id;
            size_t numFiles;
            Project * originalProject;
            Commit * originalCommit;
            std::string originalRoot;
        }; // Clone


        class Analyzer {



            



            
            
        };

        


        
    } // anonymous namespace

    






    void FolderCloneHistoryAnalysis(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();
        
    }

    
} // namespace dejavu
