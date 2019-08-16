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

        
        class Normalizer {
        public:
            void loadSuperset() {
                std::cerr << "Loading superset times..." << std::endl;
                StringRowLoader{Input.value() + "/historyOverview.csv", [this](std::vector<std::string> const & row) {
                        times_.insert(std::stoull(row[0]));
                    }};
            }

            void normalize() {
                std::cerr << "Loading data..." << std::endl;
                StringRowLoader{DataDir.value() + "/historyOverview.csv", [this](std::vector<std::string> const & row) {
                        uint64_t time = std::stoull(row[0]);
                        Stats stats;
                        stats.projects = std::stoul(row[1]);
                        stats.commits = std::stoul(row[2]);
                        stats.changes = std::stoul(row[3]);
                        stats.deletions = std::stoul(row[4]);
                        stats.paths = std::stoul(row[5]);
                        stats.hashes = std::stoul(row[6]);
                        stats.originals = std::stoul(row[7]);
                        stats_.insert(std::make_pair(time, stats));
                    }};
                std::cerr << "Normalizing..." << std::endl;
                std::ofstream f(DataDir.value() + "/historyOverview.csv");
                f << "time,projects,commits,changes,deletions,paths,hashes,originals" << std::endl;
                auto w = times_.begin();
                Stats last;
                auto r = stats_.begin();
                while (w != times_.end()) {
                    if (r != stats_.end() && *w == r->first) {
                        last = r->second;
                        ++r;
                    }
                    
                    f << *w << "," << last << std::endl;
                    ++w;
                }
            }


        private:
            std::set<uint64_t> times_;
            std::map<uint64_t, Stats> stats_;

        };

        
    }

    void HistoryOverviewExpander(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(Input);
        Settings.parse(argc, argv);
        Settings.check();

        Normalizer n;
        n.loadSuperset();
        n.normalize();
    }
} 
