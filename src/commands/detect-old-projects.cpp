#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"


namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }

        };

        
        class OldProjectsDetector {
        public:

            void loadData() {
                // we need to know commit times
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                    }};
                // now load all changes and if we see commit that is older than the threshold value, remember the commit
                std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Commit * c = commits_[commitId];
                        if (c->time < Threshold.value())
                            oldProjects_.insert(projectId);
                    }};
                std::cerr << "    " << oldProjects_.size() << " old projects detected" << std::endl;
            }

            void output() {
                std::cerr << "Writing results..." << std::endl;
                std::ofstream f(DataDir.value() + "/oldProjects.csv");
                f <<"projectId" << std::endl;
                for (unsigned i : oldProjects_)
                    f << i << std::endl;
            }
        private:
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_set<unsigned> oldProjects_;
        };
    }

    /** Detects projects older than given threshold and outputs their list so that they can be removed later.

        TODO make this to actually do the older & newer too, so that we have the range we want to analyze? 
     */
    void DetectOldProjects(int argc, char * argv[]) {
        Threshold.updateDefaultValue(1199145600); // beginning of the year 2008 when github was created
        Settings.addOption(DataDir);
        Settings.addOption(Threshold);
        Settings.parse(argc, argv);
        Settings.check();

        OldProjectsDetector opd;
        opd.loadData();
        opd.output();
        
        
    }
}
