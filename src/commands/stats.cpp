#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {

        class StatsCalculator {
        public:
            void doStuff() {
                std::cerr << "Loading file changes ... " << std::endl;
                size_t totalChanges = 0;
                FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        ++totalChanges;
                        contentsInProjects_[contentsId].insert(projectId);
                        uint64_t idx = (static_cast<uint64_t>(projectId) << 32) + pathId;
                        auto i = projectPathChanges_.find(idx);
                        if (i == projectPathChanges_.end())
                            projectPathChanges_.insert(std::make_pair(idx, 1));
                        else
                            ++(i->second);
                    }};
                std::cerr << "    " << totalChanges << " total changes read" << std::endl;
                std::cerr << "    " << contentsInProjects_.size() << " unique contents ids" << std::endl;
                std::cerr << "    " << projectPathChanges_.size() << " unique paths in projects" << std::endl;
            }

            void report() {
                {
                    std::ofstream f(DataDir.value() + "/stats_contentsInProjects.csv");
                    f << "#contentsId,numProjects" << std::endl;
                    size_t inMultiple = 0;
                    for (auto i : contentsInProjects_) {
                        if (i.second.size() > 1)
                            ++inMultiple;
                        f << i.first << "," << i.second.size() << std::endl;
                    }
                    std::cerr << "    " << inMultiple << " contents appear in multiple projects" << std::endl;
                }
                {
                    std::ofstream f(DataDir.value() + "/stats_projectPathChanges.csv");
                    f << "#projectId,pathId,numChanges" << std::endl;
                    size_t totalChanges = 0;
                    size_t moreThan1 = 0;
                    for (auto i : projectPathChanges_) {
                        unsigned pid = i.first >> 32;
                        unsigned pathId = i.first & 0xffffffff;
                        totalChanges += i.second;
                        if (i.second > 1)
                            ++moreThan1;
                        f << pid << "," << pathId << "," << i.second << std::endl;
                    }
                    double avgChanges = static_cast<double>(totalChanges) / projectPathChanges_.size();
                    std::cerr << "    " << moreThan1 << " project paths with more than 1 change" << std::endl;
                    std::cerr << "    " << avgChanges << " average changes per project path" << std::endl;
                }

                
            }



        private:
            // contentsId -> set of projects it appears in
            std::unordered_map<unsigned, std::unordered_set<unsigned>> contentsInProjects_;

            // project + path -> number of changes 
            std::unordered_map<uint64_t, unsigned> projectPathChanges_;


        }; 


    }



    /** Calculates various interesting stats.
     */
    void Stats(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        StatsCalculator s;
        s.doStuff();
        s.report();


        
    }


    
} // namespace dejavu
