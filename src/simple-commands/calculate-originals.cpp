#include "simple-commands.h"

#include "../settings.h"
#include "src/objects.h"

namespace dejavu {
    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", {"-o"}, false);


        size_t identicalCommitTimes = 0;
        
        struct OriginalityInfo {
            Commit * creator;
            unsigned occurences;
            std::unordered_set<unsigned> paths;
            std::unordered_set<unsigned> commits;
            std::unordered_set<unsigned> projects;
            // in case there are multiple different *oldest* commits for the snapshot
            unsigned creatorCommits;

            OriginalityInfo():
                creator(nullptr),
                occurences(0),
                creatorCommits(0) {
            }

            void update(unsigned snapshotId, unsigned commitId, unsigned pathId) {
                ++occurences;
                paths.insert(pathId);
                commits.insert(commitId);
                Commit * c = Commit::Get(commitId);
                if (c == nullptr)
                    ERROR("Unable to find commit id " << commitId);
                if (c == creator)
                    return;
                if (creator == nullptr || creator->time > c->time) { 
                    creator = c;
                    creatorCommits = 1;
                } else if (creator->time == c->time) {
                    ++identicalCommitTimes;
                    ++creatorCommits;
                }
            }
        }; // OriginalityInfo

        
        class Originals : public FileRecord::Reader {
        public:
        protected:
            void onRow(unsigned projectId, unsigned pathId, unsigned snapshotId, unsigned commitId) override {
                if (snapshotId != Snapshot::DELETED) {
                    assert(Snapshot::Get(snapshotId) != nullptr);
                    originals_[snapshotId].update(snapshotId, commitId, pathId);
                }
            }

            void onDone(size_t numRows) override {
                size_t unique = 0;
                size_t originals= 0;
                size_t copies= 0;
                std::cerr << "Rows:                   " << numRows << std::endl;
                std::cerr << "Identical commit times: " << identicalCommitTimes << std::endl;
                std::cerr << "Snapshots analyzed:     " << originals_.size() << std::endl;
                // update the snapshots
                for (auto i : originals_) {
                    Snapshot * s = Snapshot::Get(i.first);
                    assert(s != nullptr && "All snapshots should be accounted for");
                    s->creatorCommit = i.second.creator->id;
                    s->occurences = i.second.occurences;
                    s->paths = i.second.paths.size();
                    s->commits = i.second.commits.size();
                    s->projects = i.second.commits.size();
                    if (s->occurences == 1) {
                        ++unique;
                    } else {
                        ++originals;
                        copies += s->occurences - 1;
                    }
                }
                std::cerr << "Unique snapshots:   " << unique << std::endl;
                std::cerr << "Original snapshots: " << originals << std::endl;
                std::cerr << "Copies snapshots:   " << copies << std::endl;
            }
        private:
            std::unordered_map<unsigned, OriginalityInfo> originals_;
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
        std::string snapshotsInput = STR(DataRoot.value() << InputDir.value() << "/fileHashes.csv");
        Snapshot::ImportFrom(snapshotsInput, false);

        std::string commitsInput = STR(DataRoot.value() << OutputDir.value() << "/commits.csv");
        Commit::ImportFrom(commitsInput, true);
    

        std::string filesInput = STR(DataRoot.value() << InputDir.value() << "/files.csv");
        Originals originals;
        originals.readFile(filesInput);

        // finally save the data
        std::string outputFile = STR(DataRoot.value() << OutputDir.value() << "/snapshots.csv");
        Snapshot::SaveAll(outputFile);
    }
    
} // namespace dejavu
