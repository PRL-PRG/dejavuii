#include <set>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        uint64_t GetYearStart(unsigned year) {
            if (year == 2019) return 1546300800;
            if (year == 2018) return 1514764800;
            if (year == 2017) return 1483228800;
            if (year == 2016) return 1451606400;
            if (year == 2015) return 1420070400;
            if (year == 2014) return 1388534400;
            if (year == 2013) return 1356998400;
            if (year == 2012) return 1325376000;
            if (year == 2011) return 1293840000;
            if (year == 2010) return 1262304000;
            if (year == 2009) return 1230768000;
            return 1199145600;
        }

        uint64_t GetYearEnd(unsigned year) {
            if (year == 2019)
                return 1554076800;
            return GetYearStart(year + 1);
        }

        /** Gets year from a timestamp. Easier than dealing with leap years and others.
         */
        unsigned GetYearFromTimestamp(uint64_t timestamp) {
            if (timestamp >= 1546300800)
                return 2019;
            if (timestamp >= 1514764800)
                return 2018;
            if (timestamp >= 1483228800)
                return 2017;
            if (timestamp >= 1451606400)
                return 2016;
            if (timestamp >= 1420070400)
                return 2015;
            if (timestamp >= 1388534400)
                return 2014;
            if (timestamp >= 1356998400)
                return 2013;
            if (timestamp >= 1325376000)
                return 2012;
            if (timestamp >= 1293840000)
                return 2011;
            if (timestamp >= 1262304000)
                return 2010;
            if (timestamp >= 1230768000)
                return 2009;
            return 2008;
        }

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }

        };

        class ActivityRecord {
        public:
            unsigned commits;
            unsigned maxDifference;

            ActivityRecord(unsigned commits = 0, unsigned maxDifference = 0):
                commits(commits),
                maxDifference(maxDifference) {
            }
        };
        
        class Project {
        public:
            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt) {
            }
            
            unsigned id;
            uint64_t createdAt;

            std::set<uint64_t> commitTimes;

            std::unordered_map<unsigned, ActivityRecord> activity;

            void analyze() {
                std::vector<uint64_t> times(commitTimes.begin(), commitTimes.end());
                size_t i = 0;
                while (i < times.size()) {
                    // analyze single year
                    unsigned year = GetYearFromTimestamp(times[i]);
                    // if this is the first commit, in the project don't start with the year's start
                    unsigned maxDifference = i == 0 ? 0 : (times[i] - GetYearStart(year));
                    unsigned commits = 1;
                    ++i;
                    while (i < times.size() && GetYearFromTimestamp(times[i]) == year) {
                        if (times[i] - times[i-1] > maxDifference)
                            maxDifference = times[i] - times[i - 1];
                        ++commits;
                        ++i;
                    }
                    if (GetYearEnd(year) - times[i-1] > maxDifference)
                        maxDifference = GetYearEnd(year) - times[i-1];
                    activity.insert(std::make_pair(year, ActivityRecord{commits, maxDifference}));
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
                        p->commitTimes.insert(c->time);
                    }};
                std::cerr << "    " << projects_.size() << " projects" << std::endl;
                std::cerr << "    " << commits_.size() << " commits" << std::endl;
            }

            void analyze() {
                std::cerr << "Analyzing projects activity..." << std::endl;
                for (auto i : projects_)
                    i.second->analyze();
                std::cerr << "Writing results..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectActivity.csv");
                f << "projectId,commits2008,dist2008,commits2009,dist2009,commits2010,dist2010,commits2011,dist2011,commits2012,dist2012,commits2013,dist2013,commits2014,dist2014,commits2015,dist2015,commits2016,dist2016,commits2017,dist2017,commits2018,dist2018,commits2019,dist2019" << std::endl;
                std::vector<unsigned> totals;
                for (unsigned y = 2008; y < 2020; ++y)
                    totals.push_back(0);
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id;
                    for (unsigned y = 2008; y < 2020; ++y) {
                        ActivityRecord & ar = p->activity[y];
                        f << "," << ar.commits << "," << ar.maxDifference;
                        if (ar.commits > 0)
                            ++totals[y - 2008];
                    }
                    f << std::endl;
                }
                std::cerr << "Totals: " << std::endl;
                for (unsigned y = 2008; y < 2020; ++y) {
                    std::cerr << "    " << y << ": " << totals[y - 2008] << std::endl;
                }
            }

            
        private:

            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            
            
            
        };
        
    }
    

    void ActiveProjectsYears(int argc, char * argv[]) {
        Threshold.updateDefaultValue(1199145600); // beginning of the year 2008 when github was created
        Settings.addOption(DataDir);
        Settings.addOption(Threshold);
        Settings.parse(argc, argv);
        Settings.check();

        Analyzer a;
        a.loadData();
        a.analyze();
    }
}
