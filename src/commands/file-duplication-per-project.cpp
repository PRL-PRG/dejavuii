/* Calculates the # of duplicate file changes and # of changes per project for the machine learning algorithm that tries to predict the interesting projects.
 */

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

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

#include "helpers/json.hpp"

namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            uint64_t time2;
            Commit(unsigned id, uint64_t time, uint64_t time2):
                BaseCommit<Commit>(id, time),
                time2(time2),
                authorId{0} {
            }

            unsigned authorId;

            bool tag = false;
        };

        class FileInfo;
        
        class Project : public FullProject<Project, Commit> {
        public:
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                FullProject<Project, Commit>(id, user, repo, createdAt),
                changesUnique{0},
                changesOriginal{0},
                changesClone{0},
                deletions{0},
                stargazers{-1},
                passive{true},
                commits3m{0},
                commits2y{0} {
            }
            size_t changesUnique;
            size_t changesOriginal;
            size_t changesClone;
            size_t deletions;
            int stargazers;
            bool passive;
            size_t commits3m;
            size_t commits2y;
            std::string language;
            std::unordered_set<unsigned> authors3m;
            std::unordered_set<unsigned> authors2y;
            uint64_t newestCommit = std::numeric_limits<uint64_t>::min();
            uint64_t oldestCommit = std::numeric_limits<uint64_t>::max();

            /** Load the stargazers from the metadata.
             */
            void updateFromMetadata(std::string const & user, std::string const & repo) {
                std::string pName = user + "_" + repo + ".json";
                std::string mFile = STR("/data/dejavu/projects-metadata/" << pName.substr(0,2) << "/" << pName);
                if (helpers::FileExists(mFile)) {
                    nlohmann::json json;
                    std::ifstream(mFile) >> json;
                    stargazers = json["stargazers_count"];
                    try {
                        language = json["language"];
                        
                    } catch (...) {
                        // don't worry
                    }
                }
            }

                void updateNewestOldestCommits(Commit * c) {
                    if (oldestCommit > c->time)
                        oldestCommit = c->time;
                    if (newestCommit < c->time)
                        newestCommit = c->time;
                }

            void updateWithCommit(Commit * c) {
                if (commits_.find(c->id) != commits_.end())
                    return;
                if (c->time <= createdAt + 3600 * 24 * 31 * 3) {
                    ++commits3m;
                    authors3m.insert(c->authorId);
                }
                if (c->time <= createdAt + 3600 * 24 * 365 * 2) {
                    ++commits2y;
                    authors2y.insert(c->authorId);
                }
                if (c->time > 1504224000) // 9/2017 (3 months before end of 2018)
                    passive = false;
                commits_.insert(c->id);
            }

            void updateChange(Commit * c, FileInfo * fi, unsigned pathId);
                
            std::unordered_set<int> commits_;
            
        };

        class FileInfo {
        public:
            Project * originalProject;
            Commit * originalCommit;
            unsigned pathId;
            size_t occurences;

            FileInfo(Project * p, Commit * c, unsigned pathId):
                originalProject{p},
                originalCommit{c},
                pathId(pathId),
                occurences{0} {
            }

            void addOccurence(Project * p, Commit * c, unsigned path) {
                ++occurences;
                // if time differs, then the older time wins
                if (c->time < originalCommit->time) {
                    originalProject = p;
                    originalCommit = c;
                } else
                // if the time is the same, but the projects are different then project id decides
                if (p->id < originalProject->id) {
                    originalProject = p;
                    originalCommit = c;
                } else 
                // otherwise pathId decides
                if (path < pathId) {
                    originalProject = p;
                    originalCommit = c;
                }
            }
        };

        void Project::updateChange(Commit * c, FileInfo * fi, unsigned pathId) {
            if (fi->occurences == 1) {
                ++changesUnique;
            } else {
                if (fi->originalProject == this && fi->originalCommit == c && fi->pathId == pathId)
                    ++changesOriginal;
                else
                    ++changesClone;
            }
        }
        
        class Analyzer {
        public:

            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo, createdAt};
                        p->updateFromMetadata(user, repo);
                        projects_.insert(std::make_pair(id, p));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime, committerTime)));
                    }};
                std::cerr << "Loading commit authors..." << std::endl;
                StringRowLoader{DataDir.value() + "/commitAuthors.csv", [this](std::vector<std::string> const & row){
                        unsigned commitId = std::stoul(row[0]);
                        unsigned authorId = std::stoul(row[1]);
                        unsigned committerId = std::stoul(row[2]);
                        auto i = commits_.find(commitId);
                        if (i != commits_.end())
                            i->second->authorId = authorId;
                    }};
                // this is very very slow, but I do not care atm
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        auto i = files_.find(contentsId);
                        if (i == files_.end()) {
                            files_.insert(std::make_pair(contentsId, new FileInfo{p, c, pathId}));
                        } else {
                            i->second->addOccurence(p,c, pathId);
                        }
                        p->updateNewestOldestCommits(c);
                    }};
                // now load the changes again, this time update the counts 
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        p->updateWithCommit(c);
                        if (contentsId == 0)
                            ++p->deletions;
                        else
                            p->updateChange(c, files_[contentsId], pathId);
                        
                    }};
            }

            void write() {
                std::cerr << "writing the data..." << std::endl;
                std::ofstream f(DataDir.value() + "/file-duplication-and-stats-per-project.csv");
                f << "projectId,createdAt,oldestCommit,newestCommit,changesUnique,changesOriginal,changesClone,deletions,commits,commits3m,commits2y, authors3m,authors2y, stargazers, passive,language" << std::endl;;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << ","
                      << p->createdAt << ","
                      << p->oldestCommit << ","
                      << p->newestCommit << ","
                      << p->changesUnique << ","
                      << p->changesOriginal << ","
                      << p->changesClone << ","
                      << p->deletions << ","
                      << p->commits_.size() << ","
                      << p->commits3m << ","
                      << p->commits2y << ","
                      << p->authors3m.size() << ","
                      << p->authors2y.size() << ","
                      << p->stargazers << ","
                      << (p->passive ? "1" : "0") << ","
                      << p->language << std::endl;
                }
            }

        private:

            std::unordered_map<int, Project *> projects_;
            std::unordered_map<int, Commit *> commits_;
            std::unordered_map<int, FileInfo*> files_;
        };
        
    }

    void FileDuplicationPerProject(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Analyzer a;
        a.loadData();
        a.write();
        
    }
    
} // namespace dejavu
