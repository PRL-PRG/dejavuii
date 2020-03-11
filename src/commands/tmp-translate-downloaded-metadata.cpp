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
                        auto ec = system(STR("cp " << source << " " << target).c_str());
                        assert(ec = EXIT_SUCCESS);
                        ++translated;
                    }, false); // no headers
                std::cout << "    " << errors <<  " errors" << std::endl;
                std::cout << "    " << translated << " metadata files translated" << std::endl;
            }

        private:
            std::unordered_map<std::string, Project *> projects_;
        };

        
        /** Translate from the project-id bases scheme to project-name scheme, which is independent of the project ids that caused problems when join script was changed.
         */
        class OwnTranslator {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{"/data/dejavuii/join/projects.csv", [this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo};
                        projects_.insert(p);
                    }};
            }

            /** We have project ids, these correspond to the projects we have

                Then we have .deleted, 
             */
            void translate() {
                size_t projects = 0;
                size_t deleted = 0;
                std::cerr << "Translating projects metadata..." << std::endl;
                for (Project * p : projects_) {
                    std::string oldPath = STR("/data/dejavu/projects-metadata-old/" << (p->id % 1000) << "/" << p->id);
                    if (!helpers::FileExists(oldPath)) {
                        oldPath += ".deleted";
                        if (!helpers::FileExists(oldPath))
                            continue;
                        ++deleted;
                    }
                    ++projects;
                    // now store the project under new name
                    std::string newFilename = STR(p->user << "_" << p->repo << ".json");
                    std::string newPath = STR("/data/dejavu/projects-metadata/" << newFilename.substr(0, 2) << "/");
                    helpers::EnsurePath(newPath);
                    assert(!helpers::FileExists(newPath + newFilename));
                    if (! system(STR("cp " << oldPath << " " << newPath << newFilename).c_str()))
                        STR("ERROR: cannot copy");
                    
                }
                std::cerr << "    " << projects_.size() << " total projects" << std::endl;
                std::cerr << "    " << projects << " projects with patched data" << std::endl;
                std::cerr << "    " << deleted << " of which should be deleted" << std::endl;
            }

        private:
            std::unordered_set<Project *> projects_;
        };
        

    }


    void tmp_TranslateDownloadedMetadata(int argc, char * argv[]) {
        //Settings.addOption(DataDir);
        //Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        OwnTranslator t;
        t.loadData();
        t.translate();
        
        
    }
}
