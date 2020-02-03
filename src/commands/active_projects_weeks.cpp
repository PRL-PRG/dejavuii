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

            unsigned author;
            unsigned committer;

        };

        class ActivityRecord {
        public:
            unsigned commits;
            unsigned changes;
            unsigned deletions;
            std::unordered_set<unsigned> authors;
            std::unordered_set<unsigned> committers;

            ActivityRecord():
                commits(0),
                changes(0),
                deletions(0) {
            }

            void updateWith(Commit * c) {
                ++commits;
                changes += c->changes.size();
                deletions += c->deletions.size();
                authors.insert(c->author);
                committers.insert(c->committer);
            }

        };
        
        class Project {
        public:
            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt),
                firstCommitTime(std::numeric_limits<uint64_t>::max()) {
            }
            
            unsigned id;
            uint64_t createdAt;
            unsigned lastWeek = 0;
            unsigned startWeek = std::numeric_limits<unsigned>::max();
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
                    // instead of start of the project, do start of 2008 as the hardcoded start since all our data analysis begins there
                    //unsigned week = (c->time - firstCommitTime) / Threshold.value();
                    unsigned week = (c->time - DATA_ANALYSIS_START) / Threshold.value();
                    if (week < startWeek)
                        startWeek = week;
                    if (week > lastWeek)
                        lastWeek = week;
                    activity[week].updateWith(c);
                }
            }

            void outputFull(std::ostream & s) {
                s << id << "," << startWeek << "," << lastWeek;
                for (unsigned i = 0, e = (DATA_ANALYSIS_END - DATA_ANALYSIS_START) / Threshold.value(); i < e; ++i) {
                    auto ar = activity.find(i);
                    if (ar == activity.end()) {
                        s << ",0,0,0,0,0";
                    } else {
                        s << ar->second.commits << ","
                          << ar->second.changes << ","
                          << ar->second.deletions << ","
                          << ar->second.authors.size() << ","
                          << ar->second.committers.size();
                    }
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
                std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;
                {
                    std::cerr << "Loading commit authors ... " << std::endl;
                    size_t records = 0;
                    CommitAuthorsLoader{[ & records, this](unsigned id, unsigned authorId, unsigned committerId){
                            auto i = commits_.find(id);
                            if (i != commits_.end()) {
                                i->second->author = authorId;
                                i->second->committer = committerId;
                                ++records;
                            }
                    }};
                    std::cerr << "    " << records << " commits updated" << std::endl;
                }
                // now load all changes and if we see commit that is older than the threshold value, remember the commit
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
                std::ofstream f(DataDir.value() + "/projectsActiveTimeSummaryDetailed.csv");
                f << "projectId,startWeek,endWeek";
                for (size_t i = 0, e = (DATA_ANALYSIS_END - DATA_ANALYSIS_START) / Threshold.value(); i < e; ++i)
                    f << STR(",commits" << i <<",changes" << i <<",deletions" << i << ",authors" << i << ",committers" << i);
                f << std::endl;
                for (auto i : projects_) {
                    i.second->analyze();
                    i.second->outputFull(f);
                    f << std::endl;
                }
            }

            void outputOverallTimes() {
                std::cerr << "Writing projects active time" << std::endl;
                std::ofstream f(DataDir.value() + "/projectsActiveTimeSummary.csv");
                f << "projectId,firstCommit,numCommits,activeTimeUnits,activeIntervalUnits" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->firstCommitTime << "," << p->commits.size() << "," << p->activity.size() << "," << (p->lastWeek - p->startWeek) << std::endl;
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
                    for (unsigned week = p->startWeek; week <= p->lastWeek; ++week) {
                        auto i = p->activity.find(week);
                        if (i != p->activity.end()) {
                            ActivityRecord const & r = i->second;
                            f << p->id << "," << (week - p->startWeek) << "," << r.commits << "," << r.changes << "," << r.deletions << std::endl;
                            commits += r.commits;
                            changes += r.changes;
                            deletions += r.deletions;
                            fc << p->id << "," << (week - p->startWeek) << "," << commits << "," << changes << "," << deletions << std::endl;
                        }
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
        Threshold.updateDefaultValue(3600 * 24 * 7); // 7 days interval
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
