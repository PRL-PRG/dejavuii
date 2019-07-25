#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"

/** Takes the downloaded github metadata as reported by Konrad's downloader v1 and outputs them in the v2 format.


    // an error, this is not a valid project in our data it seems
    Project sghosh79/rolodex not found

 */

namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            std::string user;
            std::string repo;
        };

        class Translator {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo};
                        projects_.insert(std::make_pair(user + "/" + repo, p));
                    }};
            }

            void translate() {
                std::cerr << "Translating downloaded metadata..." << std::endl;
                size_t translated = 0;
                size_t errors = 0;
                std::unordered_set<unsigned> createdPaths;
                StringRowLoader(DataDir.value() + "/repository_details/__downloaded.csv",[&, this](std::vector<std::string> const & row) {
                        assert(row.size() == 1);
                        auto i = projects_.find(row[0]);
                        if (i == projects_.end()) {
                            ++errors;
                            std::cerr << "Project " << row[0] << " not found" << std::endl;
                            return;
                        }
                        Project * p = i->second;
                        std::string source = STR(DataDir.value() << "/repository_details/" << p->user << "_" << p->repo << ".json");
                        std::string targetDir = STR(OutputDir.value() << "/" << (p->id % 1000));
                        std::string target = STR(targetDir << "/" << p->id);
                        if (createdPaths.find(p->id % 1000) == createdPaths.end()) {
                            helpers::EnsurePath(targetDir);
                            createdPaths.insert(p->id % 1000);
                        }
                        system(STR("cp " << source << " " << target).c_str());
                        ++translated;
                    }, false); // no headers
                std::cout << "    " << errors <<  " errors" << std::endl;
                std::cout << "    " << translated << " metadata files translated" << std::endl;
            }

        private:
            std::unordered_map<std::string, Project *> projects_;
        };
        
    }


    void tmp_TranslateDownloadedMetadata(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        Translator t;
        t.loadData();
        t.translate();
        
        
    }
}
