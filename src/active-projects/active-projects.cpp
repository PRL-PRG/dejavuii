#include <set>
#include <unordered_set>
#include <iostream>

#include "helpers/strings.h"

#include "../settings.h"
#include "../objects.h"



#include "active-projects.h"

namespace dejavu {

    namespace {

        helpers::Option<std::string> InputDir("inputDir", "/filtered", {"-i"}, false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", {"-o"}, false);
        helpers::Option<unsigned> MaxDelay("maxDelay", 30, false); // 30 days


        class ProjectInfo {
        public:
            unsigned watchers = 0; // ok
            unsigned changes = 0;
            unsigned uniqueSnapshots = 0; // ok
            unsigned originalSnapshots = 0; // ok
            unsigned uniqueCommits = 0; // ok
            unsigned originalCommits = 0; // ok
            uint64_t spanStart = 0;
            uint64_t spanEnd = 0;
            unsigned spanCommits = 0;
            unsigned spanCount = 0;



            std::unordered_set<unsigned> paths;
            std::unordered_set<unsigned> snapshots;
            // we have definition of std::less<Commit *> in objects.h
            std::set<Commit *> commits;

            void updateLongestSpan(uint64_t start, uint64_t end, unsigned numCommits) {
                //                std::cout << start << ", " << end << ", " << numCommits << std::endl;
                if (numCommits > spanCommits || (end - start > spanEnd - spanStart)) {
                    spanStart = start;
                    spanEnd = end;
                    spanCommits = numCommits;
                }
                if (numCommits > 0)
                    ++spanCount;
            }

            friend std::ostream & operator << (std::ostream & s, ProjectInfo const & p) {
                s << p.watchers << ","
                  << p.changes << ","
                  << p.snapshots.size() << ","
                  << p.uniqueSnapshots << ","
                  << p.originalSnapshots << ","
                  << p.commits.size() << ","
                  << p.uniqueCommits << ","
                  << p.originalCommits << ","
                  << p.paths.size() << ","
                  << p.spanStart << "," << p.spanEnd << "," << p.spanCommits << ","
                  << p.spanCount << ","
                  << (*p.commits.begin())->time << ","
                  << (*p.commits.rbegin())->time;
                return s;
            }

        };


        class ProjectActivityCalculator : public FileRecord::Reader {
        public:
            void sumProjectInfo() {
                std::cerr << "Summarizing project information..." << std::endl;
                for (auto i : Project::AllProjects()) {
                    ProjectInfo * pi = new ProjectInfo();
                    projectInfo_.insert(std::make_pair(i.first, pi));
                    pi->watchers = i.second->watchers;
                }
            }

            void sumCommitInfo() {
                std::cerr << "Summarizing commit information..." << std::endl;
                for (auto i : Commit::AllCommits()) {
                    ProjectInfo * pi = projectInfo_[i.second->originalProject];
                    if (i.second->numProjects == 1)
                        ++pi->uniqueCommits;
                    else
                        ++pi->originalCommits;
                }
            }

            void sumSnapshotInfo() {
                std::cerr << "Summarizing snapshot information..." << std::endl;
                for (auto i : Snapshot::AllSnapshots()) {
                    ProjectInfo * pi = projectInfo_[Commit::Get(i.second->creatorCommit)->originalProject];
                    if (i.second->occurences == 1) {
                        ++pi->uniqueSnapshots;
                    } else {
                        ++pi->originalSnapshots;
                    }
                }
            }

            void output(std::string const & filename) {
                std::cerr << " Writing results..." << std::endl;
                std::ofstream s(filename);
                if (! s.good())
                    ERROR("Unable to open file " << filename);
                s << "id,watchers,changes,snapshots,uniqueSnapshots,originalSnapshots,commits,uniqueCommits,originalCommits,paths,spanStart,spanEnd,spanCommits,numSpans,firstCommit,lastCommit" << std::endl;
                for (auto i : projectInfo_) {
                    s << i.first << "," << (*i.second) << std::endl;
                }
            }
            
        protected:
            void onRow(unsigned pid, unsigned pathId, unsigned snapshotId, unsigned commitId) override {
                ProjectInfo * p = projectInfo_[pid];
                p->commits.insert(Commit::Get(commitId));
                p->paths.insert(pathId);
                ++p->changes;
                if (snapshotId != 0)
                    p->snapshots.insert(snapshotId);
            }

            void onDone(size_t numRows) override {
                std::cout << "Analyzed " << numRows << " file records" << std::endl;
                std::cout << "Calculating spans..." << std::endl;
                unsigned maxDelay = MaxDelay.value() * 3600 * 24; 
                for (auto i : projectInfo_) {
                    ProjectInfo * p = i.second;
                    uint64_t spanStart = 0;
                    unsigned spanCommits = 0;
                    uint64_t lastTime = 0;
                    assert(p->commits.size() > 0);
                    for (Commit * c : p->commits) {
                        assert(c->time >= lastTime);
                        if (lastTime + maxDelay < c->time) {
                            p->updateLongestSpan(spanStart, lastTime, spanCommits);
                            spanStart = c->time;
                            spanCommits = 0;
                        }
                        lastTime = c->time;
                        ++spanCommits; 
                    }
                    p->updateLongestSpan(spanStart, lastTime, spanCommits);
                }
            }




        private:
            std::unordered_map<unsigned, ProjectInfo *> projectInfo_;


            
        }; 

        /*

        class ProjectInfo2 {
        public:
            unsigned fileChanges = 0;
            unsigned spanStart = 0;
            unsigned spanEnd = 0;
            unsigned spanCommits = 0;
            std::unordered_set<unsigned> snapshots;
            std::unordered_set<unsigned> paths;
            std::set<Commit *> commits;
            /*
            bool determineLongestCommit() {
                unsigned maxDelay = MaxDelay.value() * 3600 * 24; // in days
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
                } */
            
        }; // ProjectInfo


        /*
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


        /*

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
                /*
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
                */ /*
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
        
    } */

    
    void DetermineActiveProjects(int argc, char * argv[]) {
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.addOption(MaxDelay);
        settings.parse(argc, argv);
        settings.check();
        // load the commits
        Commit::ImportFrom(DataRoot.value() + InputDir.value() + "/commits.csv", true);
        Project::ImportFrom(DataRoot.value() + InputDir.value() +"/projects.csv", true);
        Snapshot::ImportFrom(DataRoot.value() + InputDir.value() + "/snapshots.csv", true);

        ProjectActivityCalculator c;
        c.sumProjectInfo();
        c.sumCommitInfo();
        c.sumSnapshotInfo();
        std::cerr << "Analyzing file records..." << std::endl;
        c.readFile(DataRoot.value() + InputDir.value() + "/files.csv");
        c.output(DataRoot.value() + OutputDir.value() + "/projects-interesting.csv");
        //        ActiveProjectsAnalyzer a;

        
        
        
    }
    
} // namespace dejavu
