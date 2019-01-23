#include <set>
#include <unordered_set>
#include <iostream>

#include "helpers/strings.h"

#include "../settings.h"
#include "../objects.h"



#include "active-projects.h"

namespace dejavu {

    namespace {

        helpers::Option<std::string> InputDir("InputDir", "/processed", {"-i"}, false);
        helpers::Option<std::string> OutputDir("outputDir", "/processed", {"-o"}, false);
        helpers::Option<unsigned> MaxDelay("maxDelay", 30, false); // 7 days
        helpers::Option<unsigned> MinSpan("minSpan", 90, false); // 3 months
        helpers::Option<unsigned> MinCommits("minCommits", 20, false); // 20 commits

        class ProjectInfo {
        public:
            unsigned fileChanges = 0;
            unsigned spanStart = 0;
            unsigned spanEnd = 0;
            unsigned spanCommits = 0;
            std::unordered_set<unsigned> snapshots;
            std::unordered_set<unsigned> paths;
            std::set<Commit *> commits;

            bool determineLongestCommit() {
                unsigned maxDelay = MaxDelay.value() * 3600 * 24; // in days
                unsigned minSpan = MinSpan.value() * 3600 * 24;
                unsigned currentStart = 0;
                unsigned currentEnd = 0;
                unsigned currentCommits = 0;
                unsigned lastTime = 0;
                for (Commit * c : commits) {
                    assert(c->time >= lastTime);
                    lastTime = c->time;
                    // or if the commit is within the max delay from last commit
                    if (currentEnd + maxDelay > c->time) {
                        currentEnd = c->time;
                        ++currentCommits;
                        //std::cout << "Extending span " << c-> time << ", commits: " << currentCommits << std::endl;
                    // the commit comes after the deadline, so we have to finish current span
                    } else {
                        //std::cout << "New span " << c->time << std::endl; 
                        if (spanStart == 0 || (currentEnd - currentStart > minSpan && currentEnd - currentStart > spanEnd - spanStart && currentCommits >= MinCommits.value())) {
                            spanStart = currentStart;
                            spanEnd = currentEnd;
                            spanCommits = currentCommits;
                        }
                        currentStart = c->time;
                        currentEnd = c->time;
                        currentCommits = 1;
                    }
                }
                //std::cout << "storing span..." << std::endl;
                if (spanStart == 0 || (currentEnd - currentStart > minSpan && currentEnd - currentStart > spanEnd - spanStart && currentCommits >= MinCommits.value())) {
                    spanStart = currentStart;
                    spanEnd = currentEnd;
                    spanCommits = currentCommits;
                }
                //std::cout << spanStart << " -- " << spanEnd << "--" << spanCommits << std::endl;
                return (spanEnd - spanStart > minSpan);
            }
            
        }; // ProjectInfo

        class OriginalityInfo {
        public:
            unsigned occurences = 0;
            Commit * creator = nullptr;
            void update(Commit * c) {
                ++occurences;
                if (creator == nullptr || creator->time < c->time)
                    creator = c;
            }
            
        }; // OriginalityInfo


        

        class ActiveProjectsAnalyzer : public FileRecord::Reader {
        protected:
            void onRow(unsigned pid, unsigned pathId, unsigned snapshotId, unsigned commitId) override {
                ProjectInfo * p = getProjectInfo(pid);
                Commit * c = Commit::Get(commitId);
                assert(p != nullptr && c != nullptr);
                p->snapshots.insert(snapshotId);
                p->paths.insert(pathId);
                p->commits.insert(c);
                //std::cout << pid << std::endl;
                OriginalityInfo * i = getOriginalityInfo(snapshotId);
                i->update(c);
                //std::cout << pid << std::endl;
            }

            void onDone(size_t numRows) {
                std::cout << "Calculating project spans..." << std::endl;
                unsigned active = 0;
                unsigned total = 0;
                for (auto pi : projects_) {
                    ProjectInfo * p = pi.second;
                    ++total;
                    if (p->determineLongestCommit())
                      ++active;
                }
                std::cout << "Total projects:  " << total << std::endl;
                std::cout << "Active projects: " << active << std::endl;
            }

        private:

            ProjectInfo * getProjectInfo(unsigned pid) {
                auto i = projects_.find(pid);
                if (i == projects_.end())
                    i = projects_.insert(std::make_pair(pid, new ProjectInfo())).first;
                return i->second;
            }

            OriginalityInfo * getOriginalityInfo(unsigned id) {
                auto i = originals_.find(id);
                if (i == originals_.end())
                    i = originals_.insert(std::make_pair(id, new OriginalityInfo())).first;
                return i->second;
            }

            std::unordered_map<unsigned, OriginalityInfo *> originals_;
            std::unordered_map<unsigned, ProjectInfo *> projects_;

            
        }; // ActiveProjectsAnalyzer
        
    }

    
    void DetermineActiveProjects(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.addOption(MaxDelay);
        settings.addOption(MinSpan);
        settings.parse(argc, argv);
        settings.check();
        // load the commits
        Commit::ImportFrom(DataRoot.value() + InputDir.value() + "/commits.csv");
        ActiveProjectsAnalyzer a;
        a.readFile(DataRoot.value() + InputDir.value() + "/files.csv");

        
        
        
    }
    
} // namespace dejavu
