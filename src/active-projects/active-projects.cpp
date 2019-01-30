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


            class CommitsOrderer {
            public:
                bool operator() (Commit * first, Commit * second) {
                    assert(first != nullptr && second != nullptr && "We don't expect null commit");
                    if (first->time != second->time)
                        return first->time < second->time;
                    else
                        return first < second; 
                }
                
            }; 
            
            // we have definition of std::less<Commit *> in objects.h
            std::set<Commit *, CommitsOrderer> commits;

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
                Commit * c = Commit::Get(commitId);
                assert(c != nullptr);
                p->commits.insert(c);
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
