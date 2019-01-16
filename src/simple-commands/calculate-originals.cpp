#include "simple-commands.h"

#include "../settings.h"
#include "../objects.h"

namespace dejavu {
    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", false);
        helpers::Option<std::string> OutputDir("outputDir", "/processed", {"-o"}, false);


        size_t identicalCommitTimes = 0;
        
        struct OriginalityInfo {
            unsigned id;
            Commit * creator;
            unsigned occurences;
            std::unordered_set<unsigned> paths;
            std::unordered_set<unsigned> commits;

            OriginalityInfo():
                id(0),
                creator(nullptr),
                occurences(0) {
            }

            void update(unsigned snapshotId, unsigned commitId, unsigned pathId) {
                id = snapshotId;
                ++occurences;
                paths.insert(pathId);
                commits.insert(commitId);
                Commit * c = Commit::Get(commitId);
                if (c == nullptr)
                    ERROR("Unable to find commit id " << commitId);
                if (c == creator)
                    return;
                if (creator == nullptr || creator->time > c->time) 
                    creator = c;
                else if (creator->time == c->time)
                    ++identicalCommitTimes;
                //std::cerr << "Two commits same time, snapshot: " << id << ", creator " << creator->id << ", other: " << commitId << std::endl;
            }

            friend std::ostream & operator << (std::ostream & s, OriginalityInfo const & o) {
                s << o.id << "," << o.occurences << "," << o.paths.size() << "," << o.commits.size() << "," << o.creator->id;
                return s;
            }
            
        }; // OriginalityInfo

        
        class Originals : public FilesImporter {
        public:
            Originals(std::string const & outputFile) {
                output_.open(outputFile);
                if (! output_.good())
                    throw std::runtime_error(STR("Unable to open file " << outputFile));
            }

        protected:
            void onRow(unsigned projectId, unsigned pathId, unsigned snapshotId, unsigned commitId) override {
                originals_[snapshotId].update(snapshotId, commitId, pathId);
            }

            void onDone(size_t numRows) override {
                size_t unique;
                size_t originals;
                size_t copies;
                std::cerr << "Rows:                   " << numRows << std::endl;
                std::cerr << "Identical commit times: " << identicalCommitTimes << std::endl;
                std::cerr << "CAN I DUMP RESULTZ?" << std::endl;
                for (auto o : originals_) {
                    if (o.second.occurences == 1) {
                        ++unique;
                    } else {
                        ++originals;
                        copies += o.second.occurences - 1;
                    }
                    output_ << o.second << std::endl;
                }
                std::cerr << "AWSHUM" << std::endl;
                std::cerr << "Unique snapshots:   " << unique << std::endl;
                std::cerr << "Original snapshots: " << originals << std::endl;
                std::cerr << "Copies snapshots:   " << copies << std::endl;
            }
        private:
            std::unordered_map<unsigned, OriginalityInfo> originals_;
            std::ofstream output_;
        }; // Originals
    } // anonymous namespace


    void CalculateOriginals(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();
        // std::string snapshotsInput = STR(DataRoot.value() << InputDir.value() << "/fileHashes.csv");
        // first import the commits we'll need to determine the originals
        std::string commitsInput = STR(DataRoot.value() << InputDir.value() << "/commits.csv");
        Commit::ImportFrom(commitsInput);

        std::string outputFile = STR(DataRoot.value() << OutputDir.value() << "/snapshots-originality.csv");
        std::string filesInput = STR(DataRoot.value() << InputDir.value() << "/files.csv");
        Originals originals(outputFile);
        originals.readFile(filesInput);
    }
    
} // namespace dejavu
