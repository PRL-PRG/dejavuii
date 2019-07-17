#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    /** Given a list of projects and a dataset creates new dataset that would not contain data from the specified projects.

        I.e. removes all commits and file changes unique to these projects. Does not change hashes, paths, or other properties, nor does it recalculate indices.
     */
    class ProjectsFilter {
    public:
        /** Loads the projects to be filtered out.
         */
        void loadFilter() {
            {
                std::cerr << "Loading projects to be filtered out..." << std::endl;
                StringRowLoader(Filter.value(), [this](std::vector<std::string> const & row) {
                        unsigned pid = std::stoul(row[0]);
                        projectsFilter_.insert(pid);
                    });
                std::cerr << "    " << projectsFilter_.size() << " projects loaded" << std::endl;
            }
        }

        /** Filters the projects.

            Keep all projects that are not in the set to filter out.
         */
        void filterProjects() {
            std::cerr << "Filtering projects..." << std::endl;
            std::ofstream f(OutputDir.value() + "/projects.csv");
            f << "projectId,user,repo,createdAt" << std::endl;
            size_t filtered = 0;
            ProjectLoader{[this, &filtered, &f](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                    // thart's the project we want to ignore
                    if (projectsFilter_.find(id) != projectsFilter_.end()) {
                        ++filtered;
                        return;
                    }
                    f << id << "," << helpers::escapeQuotes(user) << "," << helpers::escapeQuotes(repo) << "," << createdAt << std::endl;
                    validProjects_.insert(id);
                }};
            std::cerr << "    " << validProjects_.size() << " valid projects" << std::endl;
            std::cerr << "    " << filtered << " projects filtered out" << std::endl;
        }

        /** Filters the file changes.

            Keep all file changes to valid projects, record valid commits. 
         */
        void filterFileChanges() {
            std::cerr << "Filtering file changes..." << std::endl;
            std::ofstream f(OutputDir.value() + "/fileChanges.csv");
            f << "projectId,commitId,pathId,contentsId" << std::endl;
            size_t total = 0;
            size_t valid = 0;
            FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                    ++total;
                    if (validProjects_.find(projectId) == validProjects_.end())
                        return;
                    ++valid;
                    validCommits_.insert(commitId);
                    f << projectId << "," << commitId << "," << pathId << "," << contentsId << std::endl;
                }};
            std::cerr << "    " << total << " file changes observed" << std::endl;
            std::cerr << "    " << valid << " file changes kept" << std::endl;
            std::cerr << "    " << validCommits_.size() << " valid commits detected" << std::endl;
        }
        
        /** Filters commits.

            Keeps all valid commits. In commit parents, keeps only records where both commit and parent are valid. 
         */
        void filterCommits() {
            {
                std::cerr << "Filtering commits..." << std::endl;
                std::ofstream f(OutputDir.value() + "/commits.csv");
                f << "commitId,authorTime,committerTime" << std::endl;
                size_t total = 0;
                size_t valid = 0;
                CommitLoader{[&,this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        ++total;
                        if (validCommits_.find(id) == validCommits_.end())
                            return;
                        ++valid;
                        f << id << "," << authorTime << "," << committerTime << std::endl;
                    }};
                std::cerr << "    " << total << " commits observed" << std::endl;
                std::cerr << "    " << valid << " commits kept" << std::endl;
            }
            {
                std::cerr << "Filtering commit parents..." << std::endl;
                std::ofstream f(OutputDir.value() + "/commitParents.csv");
                f << "commitId,parentId" << std::endl;
                size_t total = 0;
                size_t valid = 0;
                CommitParentsLoader{[&, this](unsigned id, unsigned parentId){
                        ++total;
                        if (validCommits_.find(id) == validCommits_.end())
                            return;
                        if (validCommits_.find(parentId) == validCommits_.end())
                            return;
                        ++valid;
                        f << id << "," << parentId << std::endl;
                    }};
                std::cerr << "    " << total << " links observed" << std::endl;
                std::cerr << "    " << valid << " links kept" << std::endl;
            }
        }


        void createSymlinks() {
            std::cerr << "Creating symlinks..." << std::endl;
            helpers::System(STR("ln -s " << DataDir.value() + "/hashes.csv " << OutputDir.value() << "/hashes.csv"));
            helpers::System(STR("ln -s " << DataDir.value() + "/paths.csv " << OutputDir.value() << "/paths.csv"));
        }




    private:
        std::unordered_set<unsigned> projectsFilter_;
        std::unordered_set<unsigned> validProjects_;
        std::unordered_set<unsigned> validCommits_;
        
    }; 

    void FilterProjects(int argc, char * argv []) {
        Settings.addOption(DataDir);
        Settings.addOption(Filter);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        helpers::EnsurePath(OutputDir.value());

        ProjectsFilter pf;
        pf.loadFilter();
        pf.filterProjects();
        pf.filterFileChanges();
        pf.filterCommits();
        pf.createSymlinks();

    }

} // namespace dejavu
