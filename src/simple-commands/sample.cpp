#include "simple-commands.h"

#include "helpers/csv-reader.h"
#include "helpers/strings.h"

#include "../settings.h"
#include "../objects.h"

namespace dejavu {

    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/filtered", false);
        helpers::Option<std::string> OutputDir("outputDir", "/sample", false);
        helpers::Option<unsigned> NumProjects("numProjects", 20000, false);


        std::unordered_set<unsigned> SampleProjects(unsigned num) {
            srand(Seed.value());
            std::unordered_set<unsigned> result;
            while (result.size() != num) {
                size_t x = rand() % Project::AllProjects().size();
                if (result.find(x) != result.end())
                    continue;
                result.insert(x);
            }
            return result;
        }

        class Sampler : public FileChange::Reader {
        public:
            Sampler(std::unordered_set<unsigned> && projects):
                projects_(std::move(projects)),
                changesOut_(DataRoot.value() + OutputDir.value() + "/files.csv") {
            }
            
        protected:

            void onRow(unsigned projectId, unsigned pathId, unsigned fileHashId, unsigned commitId) override {
                if (projects_.find(projectId) != projects_.end()) {
                    ++sampledChanges_;
                    commits_.insert(commitId);
                    paths_.insert(pathId);
                    if (fileHashId != 0)
                        snapshots_.insert(fileHashId);
                    changesOut_ << projectId << "," << pathId << "," << fileHashId << "," << commitId << std::endl;
                }
            }

            void onDone(size_t numRows) override {
                std::cout << "Analyzed " << numRows << " changes, retained " << sampledChanges_ << std::endl;
                std::cout << "Retained commits: " << commits_.size() << std::endl;
                std::cout << "Retained paths:" << paths_.size() << std::endl;
                std::cout << "Retained snapshots: " << snapshots_.size() << std::endl;
                std::cout << "Retained projects: " << projects_.size() << std::endl;  
                {
                    std::cout << "Writing projects..." << std::endl;
                    std::ofstream s(DataRoot.value() + OutputDir.value() + "/projects.csv");
                    s << "pid,user,repo,createdAt,fork,committers,authors,watchers" << std::endl;
                    for (unsigned i : projects_) {
                        s << *(Project::Get(i)) << std::endl;
                    }
                }
                {
                    std::cout << "Writing commits..." << std::endl;
                    std::ofstream s(DataRoot.value() + OutputDir.value() + "/commits.csv");
                    s << "id,hash,time,numProjects,originalProjectId" << std::endl;    
                    for (unsigned i : commits_) {
                        s << *(Commit::Get(i)) << std::endl;
                    }
                }
                {
                    std::cout << "Writing paths..." << std::endl;
                    std::ofstream s(DataRoot.value() + OutputDir.value() + "/paths.csv");
                    for (unsigned i : paths_) {
                        s << *(Path::Get(i)) << std::endl;
                    }
                }
                {
                    std::cout << "Writing snapshots..." << std::endl;
                    std::ofstream s(DataRoot.value() + OutputDir.value() + "/snapshots.csv");
                    s << "id,hash,creatorCommit,occurences,paths,commits,projects" << std::endl;    
                    for (unsigned i : snapshots_) {
                        s << *(FileHash::Get(i)) << std::endl;
                    }
                }
                
            }

        private:
            size_t sampledChanges_ = 0;
            std::unordered_set<unsigned> commits_;
            std::unordered_set<unsigned> paths_;
            std::unordered_set<unsigned> snapshots_;
            std::unordered_set<unsigned> projects_;

            std::ofstream changesOut_;
        }; 

        
    }

    void Sample(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.addOption(NumProjects);
        settings.parse(argc, argv);
        settings.check();

        Project::ImportFrom(DataRoot.value() + InputDir.value() +"/projects.csv", true);
        Commit::ImportFrom(DataRoot.value() + InputDir.value() + "/commits.csv", true);
        Path::ImportFrom(DataRoot.value() + InputDir.value() + "/paths.csv");
        FileHash::ImportFrom(DataRoot.value() + InputDir.value() + "/snapshots.csv", true);

        helpers::EnsurePath(DataRoot.value() + OutputDir.value());

        Sampler s(SampleProjects(NumProjects.value()));
        s.readFile(DataRoot.value() + InputDir.value() + "/files.csv");

        
        
        
    }
    
} // namespace dejavu
