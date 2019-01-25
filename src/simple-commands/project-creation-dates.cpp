#include "helpers/csv-reader.h"
#include "helpers/strings.h"

#include "../settings.h"
#include "../objects.h"
#include "simple-commands.h"

namespace dejavu {
    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", {"-if"}, false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", {"-of"}, false);
        helpers::Option<std::string> GhTorrentDir("ghtDir", "/array/dejavu/ghtorrent/mysql-2019-01-01", false);
        helpers::Option<std::string> Language("language", "JavaScript", false);

        

        class GHProjectsReader : public helpers::CSVReader {
        public:
            /** Returns map that takes GHTorrent project Id's to our project ids.
             */
            static std::unordered_map<unsigned, unsigned> AddProjectExtras() {
                GHProjectsReader r;
                std::cout << "Creating projects map based on project names..." << std::endl;
                for (auto i : Project::AllProjects()) {
                    std::string name = i.second->user + "/" + i.second->repo;
                    r.projects_[name] = i.second;
                }
                // now parse the ghtorrent file to fill in the extra info
                std::cout << "Analyzing GH torrent projects..." << std::endl;
                r.parse(GhTorrentDir.value() + "/projects.csv", false);
                std::cout << "Total projects:    " << r.totalProjects_ << std::endl;
                std::cout << "Language projects: " << r.languageProjects_ << std::endl;
                std::cout << "Missing projects:  " << r.missingProjects_ << std::endl;
                std::cout << "Missing forks:     " << r.missingForks_ << std::endl;
                std::cout << "Missing deleted:   " << r.missingDeleted_ << std::endl;
                // recalculate the forks from ghprojects to our project ids
                r.recalculateProjectForkIds();

                return std::move(r.ghToOurId_);
            }
            
        protected:

            void recalculateProjectForkIds() {
                uint64_t missingForks = 0;
                uint64_t unaccounted = 0;
                uint64_t forks = 0;
                uint64_t projects = 0;
                for (auto i : Project::AllProjects()) {
                    if (i.second->fork >= 0) {
                        auto j = ghToOurId_.find(i.second->fork);
                        if (j == ghToOurId_.end()) {
                            ++missingForks;
                            i.second->fork = Project::UNKNOWN_FORK;
                        } else {
                            i.second->fork = j->second;
                            ++forks;
                        }
                    }
                    if (i.second->createdAt == 0)
                        ++unaccounted;
                    ++projects;
                }
                std::cout << "Projects:      " << projects << std::endl;
                std::cout << "Unaccounted:   " << unaccounted << std::endl;
                std::cout << "Forks:         " << forks << std::endl;
                std::cout << "Missing forks: " << missingForks << std::endl;
            }

            
            // 0  1   2        3    4          5        6          7           8       9
            // id url owner_id name descriptor language created_at forked_from deleted updated_at
            void row(std::vector<std::string> & row) override {
                ++totalProjects_;
                if (row[5] != Language.value()) {
                    //                    std::cout << row[5] << std::endl;
                    return;
                }
                ++languageProjects_;
                std::string name = row[1].substr(29);
                auto i = projects_.find(name);
                if (i == projects_.end()) {
                    ++missingProjects_;
                    if (row[7] != "\\N")
                        ++missingForks_;
                    else if (row[8] != "0")
                        ++missingDeleted_;
                    return;
                }
                i->second->createdAt = helpers::StrToTimestamp(row[6]);
                if (row[7] == "\\N") 
                    i->second->fork = Project::NO_FORK;
                else
                    i->second->fork = std::stoul(row[7]);
                    
                ghToOurId_[std::stoul(row[0])] = i->second->id;
            }

            std::unordered_map<std::string, Project *> projects_;
            std::unordered_map<unsigned, unsigned> ghToOurId_;

            uint64_t totalProjects_ = 0;
            uint64_t languageProjects_ = 0;
            uint64_t missingProjects_ = 0;
            uint64_t missingForks_ = 0;
            uint64_t missingDeleted_ = 0;
            
        }; // GHProjectsReader


        class GHTCommitsReader : public helpers::CSVReader {
        public:
            static void CalculateAuthorsAndCommitters(std::unordered_map<unsigned, unsigned> const & ghToOurs) {
                GHTCommitsReader r(ghToOurs);
                r.parse(GhTorrentDir.value() + "/commits.csv", false);
                std::cout << "Commits analyzed:  " << r.commits_ << std::endl;
                std::cout << "Commits skipped:  " << r.skipped_ << std::endl;
                std::cout << "Affected projects: " << r.projects_.size() << std::endl;
                for (auto i : r.projects_) {
                    Project * p = Project::Get(i.first);
                    assert(p != nullptr);
                    p->committers = i.second.committers.size();
                    p->authors = i.second.authors.size();
                }
            }

        protected:

            GHTCommitsReader(std::unordered_map<unsigned, unsigned> const & ghToOurs):
                ghToOurs_(ghToOurs),
                commits_(0),
                skipped_(0) {
            }


            
            // 0  1   2         3            4          5
            // id sha author_id committer_id project_id created_at
            void row(std::vector<std::string> & row) override {
                if (row[4] == "\\N") {
                    ++skipped_;
                    return;
                }
                //                std::cout << row[4] << "," << row[2] << "," << row[3] << std::endl;
                unsigned projectId = std::stoul(row[4]);
                auto i = ghToOurs_.find(projectId);
                if (i != ghToOurs_.end()) {
                    ProjectInfo & pi = projects_[i->second];
                    pi.authors.insert(std::stoul(row[2]));
                    pi.committers.insert(std::stoul(row[3]));
                    ++commits_;
                }
            }

        private:
            struct ProjectInfo {
                std::unordered_set<unsigned> authors;
                std::unordered_set<unsigned> committers;
            };

            std::unordered_map<unsigned, unsigned> const & ghToOurs_;
            uint64_t commits_;
            uint64_t skipped_;
            std::unordered_map<unsigned, ProjectInfo> projects_;
            
        };

        class GHTWatchersReader : public helpers::CSVReader {
        public:
            static void CalculateWatchers(std::unordered_map<unsigned, unsigned> const & ghToOurs) {
                GHTWatchersReader r(ghToOurs);
                r.parse(GhTorrentDir.value() + "/watchers.csv", false);
                std::cout << "Rows used:          " << r.numRows_ << std::endl;
                std::cout << "Affected projects: " << r.projects_.size() << std::endl;
                for (auto i : r.projects_) {
                    Project * p = Project::Get(i.first);
                    assert(p != nullptr);
                    p->watchers = i.second.size();
                }
            }

        protected:
            // 0       1       2         
            // repo_id user_id created_at
            void row(std::vector<std::string> & row) override {
                unsigned projectId = std::stoul(row[0]);
                auto i = ghToOurs_.find(projectId);
                if (i != ghToOurs_.end()) {
                    unsigned uid = std::stoul(row[1]);
                    projects_[i->second].insert(uid);
                    ++numRows_;
                }
            }

            GHTWatchersReader(std::unordered_map<unsigned, unsigned> const & ghToOurs):
                ghToOurs_(ghToOurs),
                numRows_(0) {
            }
            
            std::unordered_map<unsigned, unsigned> const & ghToOurs_;
            std::unordered_map<unsigned, std::unordered_set<unsigned>> projects_;
            uint64_t numRows_;
        };
        
    } // anonymous namespace


    // TODO rename to project extras ?
    void ProjectCreationDates(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.addOption(GhTorrentDir);
        settings.addOption(Language);
        settings.parse(argc, argv);
        settings.check();
        // import the projects
        Project::ImportFrom(DataRoot.value() + InputDir.value() + "/projects.csv", false);
        // now read the ghtorrent stuffs and load the 
        std::unordered_map<unsigned, unsigned> ghToOurs = GHProjectsReader::AddProjectExtras();
        GHTCommitsReader::CalculateAuthorsAndCommitters(ghToOurs);
        GHTWatchersReader::CalculateWatchers(ghToOurs);
        // output the results
        helpers::EnsurePath(DataRoot.value() + OutputDir.value());
        Project::SaveAll(DataRoot.value() + OutputDir.value() + "/projects.csv");
    }
    
} // namespace dejavu
