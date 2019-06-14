#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"


namespace dejavu {

    namespace {

        class Commit {
        };

        class Project : public BaseProject<Project, Commit> {
        public:

            Project * forkedFrom;
            
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }
        };


        
        class Patcher {
        public:

            void loadData() {
                std::cerr << "Loading projects ..." << std::endl;
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        // projects_[id] = new Project(id, createdAt);
                    }};
                
                std::cerr << "Loading GHTorrent projects..." << std::endl;
                GHTorrentProjectsLoader loader([this](unsigned id,
                                                      std::string const & url,
                                                      unsigned ownerId,
                                                      std::string const & name,
                                                      std::string const & description,
                                                      std::string const & language,
                                                      uint64_t createdAt,
                                                      unsigned forkedFrom,
                                                      uint64_t deleted,
                                                      uint64_t updatedAt){
                                                   
                                               });
            }

        private:
            
            std::unordered_map<std::string, Project *> projects_;
            

            
        }; // Patcher

        
        
    }

    


    void PatchProjectsCreatedAt(int argc, char * argv []) {
        Settings.addOption(DataDir);
        Settings.addOption(GhtDir);
        Settings.parse(argc, argv);
        Settings.check();

        Patcher p;
        p.loadData();


        
    }
    
} // namespace dejavu


