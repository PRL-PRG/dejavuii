#include <set>
#include <unordered_set>

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

        class ActivityRecord {
        public:
            unsigned commits;
            unsigned changes;
            unsigned deletions;

            ActivityRecord():
                commits(0),
                changes(0),
                deletions(0) {
            }

            void updateWith(Commit * c) {
                ++commits;
                changes += c->changes.size();
                deletions += c->deletions.size();
            }

        };
        
        class Project {
        public:
            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt),
                activeWeeks(0),
                firstCommitTime(std::numeric_limits<uint64_t>::max()) {
            }
            
            unsigned id;
            uint64_t createdAt;
            unsigned activeWeeks;
            uint64_t firstCommitTime;

            std::unordered_set<Commit *> commits;


            void addCommit(Commit * c) {
                if (c->time < firstCommitTime)
                    firstCommitTime = c->time;
                commits.insert(c);
            }

            std::unordered_map<unsigned, ActivityRecord> activity;

            void analyze() {
                for (Commit * c : commits) {
                    unsigned week = (c->time - firstCommitTime) / Threshold.value();
                    if (week > activeWeeks)
                        activeWeeks = week;
                    activity[week].updateWith(c);
                }
            }
        };
        

        /**
         */
        class Analyzer {
        public:
            void loadData() {
                // just get projects
                std::cerr << "Loading projects..." << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                // we need to know commit times
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                    }};
                // now load all changes and if we see commit that is older than the threshold value, remember the commit
                std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        c->addChange(pathId, contentsId);
                        p->addCommit(c);
                    }};
                std::cerr << "    " << projects_.size() << " projects" << std::endl;
                std::cerr << "    " << commits_.size() << " commits" << std::endl;
            }

            void analyze() {
                std::cerr << "Analyzing projects activity..." << std::endl;
                for (auto i : projects_)
                    i.second->analyze();
            }

            void outputOverallTimes() {
                std::cerr << "Writing projects active time" << std::endl;
                std::ofstream f(DataDir.value() + "/projectsActiveTimeSummary.csv");
                f << "projectId,firstCommit,numCommits,activeTimeUnits,activeIntervalUnits" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->firstCommitTime << "," << p->commits.size() << "," << p->activity.size() << "," << p->activeWeeks << std::endl;
                }
            }

            void outputWeeklySummaries() {
                std::cerr << "Writing weekly activity details" << std::endl;
                std::ofstream f(DataDir.value() + "/projectsActivityTimeSummaries.csv");
                f << "projectId,timeUnit,commits,changes,deletions" << std::endl;
                std::ofstream fc(DataDir.value() + "/projectsActivityTimeCummulativeSummaries.csv");
                fc << "projectId,timeUnit,commits,changes,deletions" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    unsigned commits = 0;
                    unsigned changes = 0;
                    unsigned deletions = 0;
                    for (unsigned week = 0; week <= p->activeWeeks; ++week) {
                        ActivityRecord const & r = p->activity[week];
                        f << p->id << "," << week << "," << r.commits << "," << r.changes << "," << r.deletions << std::endl;
                        commits += r.commits;
                        changes += r.changes;
                        deletions += r.deletions;
                        fc << p->id << "," << week << "," << commits << "," << changes << "," << deletions << std::endl;
                    }
                    p->activity.clear();
                }
            }
            
        private:

            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            
            
            
        };
        
    }
    

    void ActiveProjectsWeeks(int argc, char * argv[]) {
        Threshold.updateDefaultValue(3600 * 24 * 30); // 30 month interval
        Settings.addOption(DataDir);
        Settings.addOption(Threshold);
        Settings.parse(argc, argv);
        Settings.check();

        Analyzer a;
        a.loadData();
        a.analyze();
        a.outputOverallTimes();
        a.outputWeeklySummaries();
    }
}
