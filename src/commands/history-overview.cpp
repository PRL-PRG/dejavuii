#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <set>
#include <atomic>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <openssl/sha.h>

#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time){
            }

        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }
        };



        class Stats {
        public:
            // diff - needs to be aggregated
            unsigned projects;
            unsigned commits;
            unsigned changes;
            unsigned deletions;

            unsigned paths;
            unsigned hashes;
            unsigned originals;

            Stats() {
                // just zero the memory
                memset(this, 0, sizeof(Stats));
            }

            void aggregateWith(Stats const & second) {
                projects += second.projects;
                commits += second.commits;
                changes += second.changes;
                deletions += second.deletions;
                paths += second.paths;
                hashes += second.hashes;
                originals += second.originals;
            }

            friend std::ostream & operator << (std::ostream & s, Stats const & stats) {
                s << stats.projects << ","
                  << stats.commits << ","
                  << stats.changes << ","
                  << stats.deletions << ","
                  << stats.paths << ","
                  << stats.hashes << ","
                  << stats.originals;
                return s;
            }
        };
       
        class Overview {
        public:
            
            void loadProjects() {
                // first load all projects, commits and file changes
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        createdAt -= createdAt % Threshold.value();
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                        ++diffStats_[createdAt].projects;
                    }};
                std::cerr << "    " << projects_.size() << " total projects" << std::endl;
            }

            void loadCommits() {
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        authorTime -= authorTime % Threshold.value();
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                        ++diffStats_[authorTime].commits;
                    }};
                std::cerr << "    " << commits_.size() << " total commits" << std::endl;
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cout << "    " << diffStats_.size() << " timepoints" << std::endl;
            }

            void loadFileChanges() {
                std::cerr << "Loading file changes ... " << std::endl;
                size_t total = 0;
                FileChangeLoader{[&, this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        if (contentsId == FILE_DELETED)
                            ++(diffStats_[c->time].deletions);
                        else
                            ++(diffStats_[c->time].changes);
                        ++total;
                    }};
                std::cerr << "    " << total << " total changes" << std::endl;
            }

            /** Calculates number of unique paths and hashes increase over time.

                This takes into account only global data, so no project specific walkthroughs are required. 
             */
            void calculateUniqueHashesAndPaths() {
                std::map<uint64_t, std::vector<Commit *>> commits;
                std::cerr << "Ordering commits..." << std::endl;
                for (auto i : commits_)
                    commits[i.second->time].push_back(i.second);
                std::cerr << "    " << commits.size() << " unique times" << std::endl;
                std::cerr << "Aggregating hashes and paths ..." << std::endl;
                std::unordered_set<unsigned> paths;
                std::unordered_set<unsigned> hashes;
                std::unordered_set<unsigned> seenHashes;
                unsigned lastPaths = 0;
                unsigned lastHashes = 0;
                unsigned lastSeenHashes = 0;
                for (auto i : commits) {
                    Stats & stats = diffStats_[i.first];
                    for (Commit * c : i.second) {
                        for (auto ch : c->changes) {
                            paths.insert(ch.first);
                            if (! hashes.insert(ch.second).second)
                                seenHashes.insert(ch.second);
                        }
                    }
                    stats.paths = paths.size() - lastPaths;
                    stats.hashes = hashes.size() - lastHashes;
                    stats.originals = seenHashes.size() - lastSeenHashes;
                    lastPaths = paths.size();
                    lastHashes = hashes.size();
                    lastSeenHashes = seenHashes.size();
                }
            }

            void aggregateAndOutput() {
                std::cerr << "Aggregating data..." << std::endl;
                std::ofstream f(DataDir.value() + "/historyOverview.csv");
                f << "time,projects,commits,changes,deletions,paths,hashes,originals" << std::endl;
                Stats x;
                for (auto i : diffStats_) {
                    x.aggregateWith(i.second);
                    f << i.first << "," << x << std::endl;
                }
            }

        private:

            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::map<uint64_t, Stats> diffStats_;
            
        };
    }

    void HistoryOverview(int argc, char * argv[]) {
        Threshold.updateDefaultValue(24 * 3600); // resolution of one day
        Settings.addOption(DataDir);
        Settings.addOption(Threshold);
        Settings.parse(argc, argv);
        Settings.check();

        Overview o;
        o.loadProjects();
        o.loadCommits();
        o.loadFileChanges();
        o.calculateUniqueHashesAndPaths();
        o.aggregateAndOutput();
    }


    
} // namespace dejavu
