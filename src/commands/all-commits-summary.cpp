#include <string>
#include <unordered_map>
#include <map>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class CummulativeInfo {
        public:
            unsigned changes;
            unsigned deletions;
            unsigned updates;

            CummulativeInfo(unsigned changes = 0, unsigned deletions = 0):
                changes{changes},
                deletions{deletions},
                updates{0} {
            }

            CummulativeInfo & operator += (CummulativeInfo const & other) {
                changes += other.changes;
                deletions += other.deletions;
                ++updates;
                return *this;
            }
        };
        
        class Commit {
        public:
            uint64_t time;

            Commit(uint64_t time):
                time(time) {
                
            }
            
            std::unordered_map<std::string, CummulativeInfo> sums;

            void add(std::string const & ext, unsigned changes, unsigned deletions) {
                sums.insert(std::make_pair(ext, CummulativeInfo{changes, deletions}));
            }

            void updateWith(Commit const & other) {
                for (auto i : other.sums)
                    sums[i.first] += i.second;
            }
        };

        class Summarizer {
        public:
            void loadCommits() {
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{DataDir.value() + "/allCommits.csv", [this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commitTimes_.insert(std::make_pair(id, authorTime));
                    }};
                std::cerr << "    " << commitTimes_.size() << " commits loaded" << std::endl;
            }

            void summarize() {
                std::cerr << "Loading cummulative commits info..." << std::endl;
                projectsAllSummary.open(DataDir.value() + "/projectsAllSummary.csv");
                projectsAllSummary << "projectId,totalCommits,jsCommits,totalChanges,jsChanges,totalDeletions,jsDeletions" << std::endl;
                projectsWeeklySummary.open(DataDir.value() + "/projectsWeeklySummary.csv");
                projectsWeeklySummary << "projectId,week,extension,commits,changes,deletions" << std::endl;
                
                currentProject_ = 0;
                CummulativeCommitsLoader{[this](size_t pid, size_t cid, std::string const & ext, unsigned changes, unsigned deletions){
                    if (pid != currentProject_) {
                        summarizeCurrentProject();
                        currentProjectCommits_.clear();
                        currentProject_ = pid;
                    }
                    // add the cumulative info to current project
                    auto i = currentProjectCommits_.find(cid);
                    if (i == currentProjectCommits_.end())
                        i = currentProjectCommits_.insert(std::make_pair(cid, Commit{commitTimes_[cid]})).first;
                    i->second.add(ext, changes, deletions);
                }};
            }

        protected:

            /** Summarize the projects week by week.
             */
            void summarizeCurrentProject() {
                if (currentProjectCommits_.empty())
                    return;
                uint64_t minCommit = std::numeric_limits<uint64_t>::max();
                size_t totalChanges = 0;
                size_t jsChanges = 0;
                size_t totalDeletions = 0;
                size_t jsDeletions = 0;
                size_t totalCommits = currentProjectCommits_.size();
                size_t jsCommits = 0;
                for (auto const & i : currentProjectCommits_) {
                    if (i.second.time < minCommit)
                        minCommit = i.second.time;
                    for (auto const & j : i.second.sums) {
                        totalChanges += j.second.changes;
                        totalDeletions += j.second.deletions;
                        if (j.first == ".js") {
                            jsChanges += j.second.changes;
                            jsDeletions += j.second.deletions;
                            ++jsCommits;
                        }
                    }
                }
                std::map<size_t, Commit> weeklySummaries;
                for (auto const &i : currentProjectCommits_) {
                    size_t week = (i.second.time - minCommit) / 3600 / 24 / 7;
                    auto j = weeklySummaries.find(week);
                    if (j == weeklySummaries.end())
                        j = weeklySummaries.insert(std::make_pair(week, Commit{week})).first;
                    j->second.updateWith(i.second);
                }
                // write the summaries for projects
                projectsAllSummary << currentProject_ << ","
                                   << totalCommits << ","
                                   << jsCommits << ","
                                   << totalChanges << ","
                                   << jsChanges << ","
                                   << totalDeletions << ","
                                   << jsDeletions << std::endl;
                // write the summaries for commits
                for (auto const & i : weeklySummaries) {
                    for (auto const &l : i.second.sums)
                        projectsWeeklySummary << currentProject_ << ","
                                              << i.first << ","
                                              << helpers::escapeQuotes(l.first) << ","
                                              << l.second.updates << ","
                                              << l.second.changes << ","
                                              << l.second.deletions << std::endl;
                }
            }

            std::unordered_map<size_t, uint64_t> commitTimes_;

            size_t currentProject_;
            std::unordered_map<size_t, Commit> currentProjectCommits_;

            std::ofstream projectsAllSummary;
            std::ofstream projectsWeeklySummary;

        }; // Summarizer

    } // anonymous namespace

    void AllCommitsSummary(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        Summarizer s;
        s.loadCommits();
        s.summarize();
    }
    

    
}
