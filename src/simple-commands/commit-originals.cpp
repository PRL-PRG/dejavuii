#include <unordered_set>

#include "../settings.h"
#include "../objects.h"


#include "simple-commands.h"

namespace dejavu {

    namespace {
        // FIXME this is ugly and confusing as hell - projects and commits may come from different directories... Eh...
        helpers::Option<std::string> ProjectsDir("projectsDir", "/filtered", false);
        helpers::Option<std::string> CommitsDir("commitsDir", "/processed", false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", {"-o"}, false);

        class CommitOriginalsLoader : public FileRecord::Reader {
        protected:
            void onRow(unsigned projectId, unsigned pathId, unsigned snapshotId, unsigned commitId) override {

                ProjectCommit x{projectId, commitId};
                if (projectCommits_.find(x) == projectCommits_.end()) {
                    projectCommits_.insert(x);
                    Commit * c = Commit::Get(commitId);
                    Project * p = Project::Get(projectId);
                    if (c->numProjects == 0) {
                        c->originalProject = projectId;
                    } else {
                        Project * po = Project::Get(c->originalProject);
                        if (p->createdAt < po->createdAt)
                            c->originalProject = projectId;
                    }
                    ++c->numProjects;
                }
            };

            void onDone(size_t rows) override {
                std::cout << "Analyzed rows: " << rows << std::endl;
                std::cout << "Analyzed project-commit pairs: " << projectCommits_.size() << std::endl;
            }

        private:

            struct ProjectCommit {
                unsigned projectId;
                unsigned commitId;

                bool operator == (ProjectCommit const & other) const {
                    return projectId == other.projectId && commitId == other.commitId;
                }
            };

            struct PCHash {
                size_t operator () (ProjectCommit const & p) const {
                    return std::hash<unsigned>()(p.projectId) + std::hash<unsigned>()(p.commitId);
                }
            };
            
            std::unordered_set<ProjectCommit, PCHash> projectCommits_;
            

            
        }; 
    } // anonymous namespace

    void CommitOriginals(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(CommitsDir);
        settings.addOption(ProjectsDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        // do the work now - import projects and commits
        Project::ImportFrom(DataRoot.value() + ProjectsDir.value() + "/projects.csv", true);
        Commit::ImportFrom(DataRoot.value() + CommitsDir.value() + "/commits.csv", false);
        // now, for each commit, determine number of projects it has and the oldest project
        CommitOriginalsLoader l;
        l.readFile(DataRoot.value() + CommitsDir.value() + "/files.csv");
        Commit::SaveAll(DataRoot.value() + OutputDir.value() + "/commits.csv");
    }
    

    
} // namespace dejavu
