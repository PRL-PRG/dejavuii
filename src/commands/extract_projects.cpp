#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {
        /** Loads the commits basic information. */
        class GHTorrentProjectLoader : public BaseLoader {
        public:

            typedef std::function<void(const std::string&, const std::string&, bool, bool)> RowHandler;

            GHTorrentProjectLoader(std::string const & filename, RowHandler f): f_(f) {
                readFile(filename);
            }

            GHTorrentProjectLoader(RowHandler f): f_(f) {
                readFile(DataDir.value() + "/ghtorrent_projects.csv");
            }

        protected:
            /**
             * [0]: id
             * [1]: url
             * [2]: owner_id
             * [3]: name
             * [4]: descriptor
             * [5]: language
             * [6]: created_at
             * [7]: forked_from
             * [8]: deleted
             * [9]: updated_at
             * [10]: ?
             */
            void row(std::vector<std::string> & row) override {
                assert((row[1] == "\\N") ||
                       (row[1].rfind("https://api.github.com/repos/", 0) == 0));
                std::string url = (row[1] == "\\N") ? "" : row[1].substr(0, 30);

                std::string language = row[5];

                bool forked = row[7] == "\\N";

                assert(row[8] == "1" || row[8] == "0");
                bool deleted = (row[8] == "1");

                f_(language, url, forked, deleted);
            }

        private:
            RowHandler f_;
        };

        class Project {
        public:
            static void LoadJSProjects() {
                std::cerr << "LOAD JS REPOS" << std::endl;
                GHTorrentProjectLoader([](const std::string &language,
                                          const std::string &repo,
                                          bool forked,
                                          bool deleted) mutable {
                    if (deleted) {
                        return;
                    }
                    if (language == "JavaScript") {
                        return;
                    }
                    if (forked) {
                        Project::forked_projects.push_back(repo);
                    } else {
                        Project::fresh_projects.push_back(repo);
                    }
                });
                std::cerr << "DONE JS LOAD REPOS" << std::endl;
            }

            static void SaveFreshJSProjects() {
                Project::_save_projects(Project::fresh_projects,
                                        DataDir.value() + "/js_projects.csv");
            }

            static void SaveForkedJSProjects() {
                Project::_save_projects(Project::forked_projects,
                                        DataDir.value() + "/js_projects_forked.csv");
            }

        private:
            static std::vector<std::string> forked_projects;
            static std::vector<std::string> fresh_projects;

            static void _save_projects(std::vector<std::string> &projects,
                                       std::string filename) {

                std::cerr << "WRITINCK OUT PROJECKT LIST TO " << filename
                          << std::endl;

                std::ofstream s(filename);
                if (! s.good()) {
                    ERROR("Unable to open file " << filename << " for writing");
                }

                int counter = 0;
                for (std::string repo : projects) {
                    s << repo << std::endl;

                    // Count processed lines
                    counter++;
                    if (counter % 1000 == 0) {
                        std::cerr << " : " << (counter / 1000) << "k\r"
                                  << std::flush;
                    }
                }
                std::cerr << " : " <<(counter / 1000) << "k" << std::endl;
                std::cerr << "DONE WRITINCK OUT PROJECKT LIST TO " << filename
                          << std::endl;
            }
        };

        std::vector<std::string> Project::forked_projects;
        std::vector<std::string> Project::fresh_projects;

    } //anonymoose namespace

    void ExtractJSProjects(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        //Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Project::LoadJSProjects();
        Project::SaveFreshJSProjects();
        Project::SaveForkedJSProjects();
    }
    
} // namespace dejavu
