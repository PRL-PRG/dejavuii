#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"


/* Jan wants:

   - # of file contents that are present in more than 1 project
 */


namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }
        };

        class Project : public FullProject<Project, Commit> {
        public:
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                FullProject<Project, Commit>(id, user, repo, createdAt) {
            }
        };

        

        class Filter {
        public:
            void filter() {
                {
                    std::cerr << "Filtering paths..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/paths.csv");
                    size_t totalPaths = 0;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            ++totalPaths;
                            if (!IsNPMPath(path)) {
                                f << id << "," << helpers::escapeQuotes(path) << std::endl;
                                paths_.insert(id);
                            }
                        }};
                    std::cerr << "    " << totalPaths << " total paths read" << std::endl;
                    std::cerr << "    " << paths_.size() << " retained paths" << std::endl;
                }
                // now that we have paths, we can load all projects and commits
                {
                    size_t totalProjects = 0;
                    std::cerr << "Loading projects ... " << std::endl;
                    ProjectLoader{[&,this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                            ++totalProjects;
                            if (id >= projects_.size())
                                projects_.resize(id + 1);
                            projects_[id] = new Project(id, user, repo,createdAt);
                        }};
                    std::cerr << "    " << totalProjects << " total projects read" << std::endl;
                }
                {
                    std::cerr << "Loading commits ... " << std::endl;
                    size_t totalCommits = 0;
                    CommitLoader{[&,this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                            ++totalCommits;
                            if (id >= commits_.size())
                                commits_.resize(id + 1);
                            commits_[id] = new Commit(id, authorTime);
                        }};
                    std::cerr << "    " << totalCommits << " total commits read" << std::endl;
                }
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                {
                    std::cerr << "Loading file changes ... " << std::endl;
                    size_t totalChanges = 0;
                    size_t validChanges = 0;
                    FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++totalChanges;
                            if (paths_.find(pathId) != paths_.end()) {
                                ++validChanges;
                                Project * p = projects_[projectId];
                                Commit * c = commits_[commitId];
                                assert(p != nullptr);
                                assert(c != nullptr);
                                p->addCommit(c);
                                c->addChange(pathId, contentsId);
                            }
                        }};
                    std::cerr << "    " << totalChanges << " total changes read" << std::endl;
                    std::cerr << "    " << validChanges << " changes retained" << std::endl;
                }
            }

            void removeEmptyCommits() {
            }
        private:



            std::unordered_set<unsigned> paths_;
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;


            
        };


        
    } //anonymous namespace

    /** Filters out any files contained in node_modules directories.
     */
    void FilterNPM(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        Filter f;
        f.filter();
    }
    
} // namespace dejavu



