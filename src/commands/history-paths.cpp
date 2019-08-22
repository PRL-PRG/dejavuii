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
/*

  Writing project aggregates...
    56077091 unique files
    12190817 original files
    1035729834 clone files

    42484606 unique files (last commit)
    6038518 original files (last commit)
    680839098 clone files (last commit)

*/



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

            unsigned uniqueFiles = 0;
            unsigned originalFiles = 0;
            unsigned cloneFiles = 0;

            unsigned finalUniqueFiles = 0;
            unsigned finalOriginalFiles = 0;
            unsigned finalCloneFiles = 0;
            
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }
        };

        class FileOriginal {
        public:
            unsigned id;
            Project * project;
            Commit * commit;
            unsigned fileId;
            unsigned numOccurences;

            FileOriginal(unsigned id, Project * project, Commit * commit, unsigned fileId):
                id(id),
                project(project),
                commit(commit),
                fileId(fileId),
                numOccurences(1) {
            }

            void update(Project * project, Commit * commit, unsigned fileId) {
                ++this->numOccurences;
                // if the new commit is newer, nothing to do
                if (commit->time > this->commit->time)
                    return;
                if (commit->time == this->commit->time) {
                    // if the commit is same age, but the project is newer or same, nothing to do
                    if (project->createdAt >= this->project->createdAt)
                        return;
                }
                this->project = project;
                this->commit = commit;
                this->fileId = fileId;
            }
        };


        enum class FileState {
            Unique,
            Original,
            Clone,
        };

        FileState & operator += (FileState & a, FileState b) {
            // if the state is altready clone, it can't change
            if (a == FileState::Clone)
                return a;
            // see if we are going to change
            switch (b) {
            case FileState::Unique:
                break;
            case FileState::Original:
            case FileState::Clone:
                a = b;
                break;
            }
            return a;
        }
        
        class FileInfo {
        public:
            unsigned creations;
            unsigned deletions;
            unsigned uniqueChanges;
            unsigned originalChanges;
            unsigned cloneChanges;
            FileState initialState;
            FileState aggregateState;

            FileInfo():
                creations(0),
                deletions(0),
                uniqueChanges(0),
                originalChanges(0),
                cloneChanges(0),
                initialState(FileState::Unique),
                aggregateState(FileState::Unique) {
            }

            void addChange(FileState state) {
                switch (state) {
                case FileState::Unique:
                    ++uniqueChanges;
                    break;
                case FileState::Original:
                    ++originalChanges;
                    break;
                case FileState::Clone:
                    ++cloneChanges;
                    break;
                }
                aggregateState += state;
            }

            void addCreation(FileState state) {
                addChange(state);
                ++creations;
                initialState += state;
            }
        };

        class State {
        public:
            State() {
            }

            State(State const & from) {
                mergeWith(from, nullptr);
            }

            void mergeWith(State const & other, Commit * c) {
                files.insert(other.files.begin(), other.files.end());
            }

            std::unordered_map<unsigned, FileState> files;
            
        }; // State

        class Stats {
        public:
            int projects;
            int uniquePaths;
            int originalPaths;
            int clonePaths;

            Stats():
                projects(0),
                uniquePaths(0),
                originalPaths(0),
                clonePaths(0) {
            }

            Stats(std::unordered_map<unsigned, FileState> const & files):
                projects(1),
                uniquePaths(0),
                originalPaths(0),
                clonePaths(0) {
                for (auto i : files) {
                    switch (i.second) {
                    case FileState::Unique:
                        ++uniquePaths;
                        break;
                    case FileState::Original:
                        ++originalPaths;
                        break;
                    case FileState::Clone:
                        ++clonePaths;
                        break;
                    }
                }
            }

            /** Returns the difference from state b.
             */
            Stats diff(Stats const & b) {
                Stats result;
                result.projects = projects - b.projects;
                result.uniquePaths = uniquePaths - b.uniquePaths;
                result.originalPaths = originalPaths - b.originalPaths;
                result.clonePaths = clonePaths - b.clonePaths;
                return result;
            }

            Stats & operator += (Stats const & other) {
                projects += other.projects;
                uniquePaths += other.uniquePaths;
                originalPaths += other.originalPaths;
                clonePaths += other.clonePaths;
                return *this;
            }

            friend std::ostream & operator << (std::ostream & s, Stats const & stats) {
                s << stats.projects << "," << stats.uniquePaths << "," << stats.originalPaths << "," << stats.clonePaths;
                return s;
            }
            
        }; // Stats

        class PathsCounter {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        createdAt -= createdAt % Threshold.value();
                        projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        authorTime -= authorTime % Threshold.value();
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                    }};
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading file changes ... " << std::endl;
                size_t numChanges = 0;
                size_t numDeletions = 0;
                FileChangeLoader{[& numChanges, & numDeletions, this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        if (contentsId == FILE_DELETED) {
                            ++numDeletions;
                        } else {
                            ++numChanges;
                            auto i = originals_.find(contentsId);
                            if (i != originals_.end())
                                i->second->update(p, c, pathId);
                            else
                                originals_.insert(std::make_pair(contentsId, new FileOriginal(contentsId, p, c, pathId)));
                        }
                    }};
                std::cerr << "    " << numDeletions << " deletions" << std::endl;
                std::cerr << "    " << numChanges << " changes" << std::endl;
                std::cerr << "    " << originals_.size() << " contents hashes" << std::endl;
                
            }

            /** This removes the unique files from the list so that we don't bother with these.
             */
            void removeUniqueFiles() {
                std::cerr << "Removing unique files..." << std::endl;
                for (auto i = originals_.begin(); i != originals_.end();) {
                    if (i->second->numOccurences == 1)
                        i = originals_.erase(i);
                    else
                        ++i;
                }
                std::cerr << "    " << originals_.size() << " non-unique content hashes (originals)" << std::endl;
            }

            void calculatePathsHistory() {
                std::cerr << "Analyzing clone behavior..." << std::endl;
                std::vector<std::thread> threads;
                auto i = projects_.begin();
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, this]() {
                        while (true) {
                            Project * p;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (i == projects_.end())
                                    return;
                                p = i->second;
                                ++i;
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            analyzeProject(p);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
            }

            void outputProjectsAggregate() {
                std::cerr << "Writing project aggregates..." << std::endl;
                std::ofstream f(DataDir.value() + "/projectPaths.csv");
                f << "projectId,uniqueFiles,originalFiles,cloneFiles" << std::endl;
                size_t uniqueFiles = 0;
                size_t originalFiles = 0;
                size_t cloneFiles = 0;
                size_t finalUniqueFiles = 0;
                size_t finalOriginalFiles = 0;
                size_t finalCloneFiles = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << p->uniqueFiles << "," << p->originalFiles << "," << p->cloneFiles << ","
                        << p->finalUniqueFiles << "," << p->finalOriginalFiles << "," << p->finalCloneFiles << std::endl;
                    uniqueFiles += p->uniqueFiles;
                    originalFiles += p->originalFiles;
                    cloneFiles += p->cloneFiles;
                    finalUniqueFiles += p->finalUniqueFiles;
                    finalOriginalFiles += p->finalOriginalFiles;
                    finalCloneFiles += p->finalCloneFiles;
                }
                std::cerr << "    " << uniqueFiles << " unique files" << std::endl;
                std::cerr << "    " << originalFiles << " original files" << std::endl;
                std::cerr << "    " << cloneFiles << " clone files" << std::endl;
                std::cerr << std::endl;
                std::cerr << "    " << finalUniqueFiles << " unique files (last commit)" << std::endl;
                std::cerr << "    " << finalOriginalFiles << " original files (last commit)" << std::endl;
                std::cerr << "    " << finalCloneFiles << " clone files (last commit)" << std::endl;
            }

            void aggregateAndOutput() {
                std::cerr << "Aggregating data..." << std::endl;
                std::ofstream f(DataDir.value() + "/historyPaths.csv");
                f << "time,projects,uniquePaths,originalPaths,clonePaths" << std::endl;
                Stats x;
                for (auto i : diffStats_) {
                    x += i.second;
                    f << i.first << "," << x << std::endl;
                }
            }
            
            
        private:

            void analyzeProject(Project * p) {
                // time -> (fileId -> status)
                std::map<uint64_t, std::unordered_map<unsigned, FileState>> files;
                // fileId -> stats about commits & deletions over time
                std::unordered_map<unsigned, FileInfo> allFiles;
                // now analyze the project commit by commit
                CommitForwardIterator<Project, Commit, State> it(p, [&, this](Commit * c, State & state){
                        // first handle all deletions, remove the file from the current state and increase the number of deletions in the project state
                        for (unsigned pathId : c->deletions) {
                            state.files.erase(pathId);
                            ++allFiles[pathId].deletions;
                        }
                        // now handle the changes
                        for (auto ch : c->changes) {
                            // get the change state and the global project file info
                            FileState st = getFileState(p, c, ch.first, ch.second);
                            FileInfo & fi = allFiles[ch.first];
                            auto i = state.files.find(ch.first);
                            // if the file does not exist in current state, it is new file, add creation to the global state
                            // and add the file to current state
                            if (i == state.files.end()) {
                                fi.addCreation(st);
                                state.files.insert(std::make_pair(ch.first, st));
                            // otherwise record the change in global state and update the current file state 
                            } else {
                                fi.addChange(st);
                                state.files[ch.first] += st;
                            }
                        }
                        // finally, for all valid files, add them to the current state at the time of the commit
                        auto & currentTime = files[c->time];
                        for (auto x : state.files) {
                            auto i = currentTime.find(x.first);
                            if (i == currentTime.end())
                                currentTime.insert(x);
                            else
                                i->second += x.second;
                        }
                        return true;
                    });
                it.process();
                // update the global diff state
                Stats last;
                for (auto i : files) {
                    uint64_t time = i.first;
                    Stats current(i.second);
                    diffStats_[time] += current.diff(last);
                    last = current;
                }
                // and update the project state & final state
                for (auto i : allFiles) {
                    switch (i.second.aggregateState) {
                    case FileState::Unique:
                        ++p->uniqueFiles;
                        break;
                    case FileState::Original:
                        ++p->originalFiles;
                        break;
                    case FileState::Clone:
                        ++p->cloneFiles;
                        break;
                    }
                }
                if (! files.empty()) {
                    for (auto i : files.rbegin()->second) {
                        switch (i.second) {
                        case FileState::Unique:
                            ++p->finalUniqueFiles;
                            break;
                        case FileState::Original:
                            ++p->finalOriginalFiles;
                            break;
                        case FileState::Clone:
                            ++p->finalCloneFiles;
                            break;
                        }
                    }
                }
            }


            FileState getFileState(Project * p, Commit * c, unsigned pathId, unsigned contentsId) {
                auto i = originals_.find(contentsId);
                if (i == originals_.end())
                    return FileState::Unique;
                if (i->second->project == p && i->second->commit == c && i->second->fileId == pathId)
                    return FileState::Original;
                return FileState::Clone;
            }


            std::mutex mCerr_;
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, FileOriginal *> originals_;
            std::map<uint64_t, Stats> diffStats_;
            
        }; // PathsCounter
    }

    void HistoryPaths(int argc, char * argv[]) {
        Threshold.updateDefaultValue(24 * 3600); // resolution of one day
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.addOption(Threshold);
        Settings.parse(argc, argv);
        Settings.check();

        PathsCounter pc;
        pc.loadData();
        pc.removeUniqueFiles();
        pc.calculatePathsHistory();
        pc.outputProjectsAggregate();
        pc.aggregateAndOutput();
    }

    
}
