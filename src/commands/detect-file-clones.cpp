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

        class FileClone {
        public:
            unsigned projectId;
            unsigned commitId;
            unsigned pathId;
            unsigned cloneId;

            FileClone(unsigned projectId, unsigned commitId, unsigned pathId, unsigned cloneId):
                projectId(projectId),
                commitId(commitId),
                pathId(pathId),
                cloneId(cloneId),
                changingCommits{0},
                divergentCommits{0},
                syncCommits{0},
                syncDelay{0},
                fullySyncedTime{0},
                fullySyncedCommits{0},
                youngestChange{0},
                youngestDivergentChange{0},
                youngestSyncChange{0} {
                
            }

            friend std::ostream & operator << (std::ostream & s, FileClone const & f) {
                s << f.projectId << "," << f.commitId << "," << f.pathId << "," << f.cloneId;
                return s;
            }

            unsigned changingCommits;
            unsigned divergentCommits;
            unsigned syncCommits;
            uint64_t syncDelay;
            uint64_t fullySyncedTime;
            unsigned fullySyncedCommits;
            uint64_t youngestChange;
            uint64_t youngestDivergentChange;
            uint64_t youngestSyncChange;
            
        };
        
        class Commit : public BaseCommit<Commit> {
        public:
            uint64_t time2;
            Commit(unsigned id, uint64_t time, uint64_t time2):
                BaseCommit<Commit>(id, time),
                time2(time2){
            }

            bool tag = false;
        };

        class Project : public FullProject<Project, Commit> {
        public:
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                FullProject<Project, Commit>(id, user, repo, createdAt) {
            }

            /** List of file clones for the project.
             */
            std::vector<FileClone*> clones;

            bool tag = false;
        };

        class FileOriginal {
        public:
            unsigned id;
            Project * project;
            Commit * commit;
            unsigned fileId;
            unsigned numOccurences;
            unsigned numClones;

            std::unordered_map<unsigned, uint64_t> contents;
            std::vector<std::pair<uint64_t, unsigned>> sortedContents;

            std::vector<FileClone*> clones;

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

            void addContents(Commit * c, unsigned contentsId) {
                auto i = contents.find(contentsId);
                if (i == contents.end())
                    contents.insert(std::make_pair(contentsId, c->time));
                else
                    if (i->second > c->time)
                        i->second = c->time;
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

            // pathId -> contentsId
            std::unordered_map<unsigned, unsigned> files;
                
        };

        class BehaviorState {
        public:
            BehaviorState():
                active(false) {
            }

            BehaviorState(BehaviorState const & from):
                active(from.active) {
            }

            void mergeWith(BehaviorState const & other, Commit * c) {
                active = active || other.active;
            }
            bool active;
        };

        class FilterState {
        public:
            FilterState() {
            }

            FilterState(FilterState const & from) {
                mergeWith(from, nullptr);
            }

            void mergeWith(FilterState const & from, Commit * c) {
                activeClones.insert(from.activeClones.begin(), from.activeClones.end());
            }

            std::unordered_set<unsigned> activeClones;
            
        }; 

        class FileClonesDetector {
        public:

            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.insert(std::make_pair(id, new Project(id, user, repo, createdAt)));
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        commits_.insert(std::make_pair(id, new Commit(id, authorTime, committerTime)));
                    }};
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        paths_[id] = path;
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
                std::cerr << "    " << originals_.size() << " unique contents (possible originals)" << std::endl;
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
                std::cerr << "    " << originals_.size() << " non-unique file contents left" << std::endl;
            }

            /** Looks for clone candidates in all loaded projects.
             */
            void detectClones() {
                std::cerr << "Analyzing projects for clone candidates..." << std::endl;
                //clonesOut_ = std::ofstream(DataDir.value() + "/fileCloneCandidates.csv");
                //clonesOut_ << "projectId,commitId,pathId,cloneId" << std::endl;
                std::vector<std::thread> threads;
                auto i = projects_.begin();
                size_t completed = 0;
                size_t numClones = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, &numClones, this]() {
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
                            detectClonesInProject(p);
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                numClones += p->clones.size();
                            }
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "    " << numClones << " file clones detected" << std::endl;
                std::cerr << "Matching with originals..." << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    for (FileClone * clone : p->clones) 
                        originals_[clone->cloneId]->clones.push_back(clone);
                }
                for (auto i = originals_.begin(), e = originals_.end(); i != e; ) {
                    if (i->second->clones.empty()) {
                        delete i->second;
                        i = originals_.erase(i);
                    } else {
                        ++i;
                    }
                }
                std::cout << "    " << originals_.size() << " valid originals" << std::endl;
            }


            void calculateBehavior() {
                std::cerr << "Analyzing clone behavior..." << std::endl;
                std::vector<std::thread> threads;
                auto i = originals_.begin();
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, &i, & completed, this]() {
                        while (true) {
                            FileOriginal * o ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (i == originals_.end())
                                    return;
                                o = i->second;
                                ++i;
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (o == nullptr)
                                continue;
                            analyzeOriginal(o);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                {
                    std::cerr << "Writing clone behavior..." << std::endl;
                    std::ofstream f(DataDir.value() + "/fileCloneOccurencesBehavior.csv");
                    f << "cloneId,projectId,commitId,pathId,path,changingCommits,divergentCommits,syncCommits,syncDelay,fullySyncedTime,fullySyncedCommits,youngestChange,youngestDivergentChange,youngestSyncChange" << std::endl;
                    for (auto i : originals_)
                        for (FileClone * c : i.second->clones)
                            f << c->cloneId << ","
                              << c->projectId << ","
                              << c->commitId << ","
                              << c->pathId << ","
                              << helpers::escapeQuotes(paths_[c->pathId]) << ","
                              << c->changingCommits << ","
                              << c->divergentCommits << ","
                              << c->syncCommits << ","
                              << c->syncDelay << ","
                              << c->fullySyncedTime << ","
                              << c->fullySyncedCommits << ","
                              << c->youngestChange << ","
                              << c->youngestDivergentChange << ","
                              << c->youngestSyncChange << std::endl;
                }
            }

            void writeOriginals() {
                std::cerr << "Writing originals information..." << std::endl;
                std::ofstream f(DataDir.value() + "/fileCloneOriginals.csv");
                f << "projectId,commitId,pathId,path,cloneId,numClones" << std::endl;
                for (auto i : originals_)
                    f << i.second->project->id << ","
                      << i.second->commit->id << ","
                      << i.second->fileId << ","
                      << helpers::escapeQuotes(paths_[i.second->fileId]) << ","
                      << i.second->id << ","
                      << i.second->clones.size() << std::endl;
            }

            void filterFileChanges() {
                std::cout << "Filtering file changes..." << std::endl;
                std::ofstream f(OutputDir.value() + "/fileChanges.csv");
                size_t changes = 0;
                size_t writtenChanges = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    std::unordered_map<Commit *, std::unordered_set<unsigned>> introducedClones;
                    for (FileClone * clone : p->clones) 
                        introducedClones[commits_[clone->commitId]].insert(clone->pathId);
                
                    CommitForwardIterator<Project, Commit, FilterState> it(p, [&, this](Commit * c, FilterState & state){
                            changes += c->deletions.size() + c->changes.size();
                            // first do deletions
                            for (unsigned pathId : c->deletions) {
                                auto j = state.activeClones.find(pathId);
                                if (j == state.activeClones.end()) {
                                    ++writtenChanges;
                                    f << p->id << "," << c->id << "," << pathId << "," << FILE_DELETED << std::endl;
                                    c->tag = true;
                                    p->tag = true;
                                } else {
                                    state.activeClones.erase(j);
                                }
                            }
                            // now add any active clones by given commit
                            {
                                auto j = introducedClones.find(c);
                                if (j != introducedClones.end())
                                    state.activeClones.insert(j->second.begin(), j->second.end());
                            }
                            // and process the changes
                            for (auto ch : c->changes) {
                                auto j = state.activeClones.find(ch.first);
                                if (j == state.activeClones.end()) {
                                    ++writtenChanges;
                                    f << p->id << "," << c->id << "," << ch.first << "," << ch.second << std::endl;
                                    c->tag = true;
                                    p->tag = true;
                                } 
                            }
                            return true;
                        });
                    it.process();
                }
                std::cerr << "    " << changes << " total changes (prior)" << std::endl;
                std::cerr << "    " << writtenChanges << " written changes" << std::endl;
            }

            /** Filtering projects is easy, we output all projects that are tagged.
             */
            void filterProjects() {
                std::cerr << "Writing filtered projects..." << std::endl;
                std::ofstream f(OutputDir.value() + "/projects.csv");
                f << "projectId,user,repo,createdAt" << std::endl;
                unsigned tagged = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    if (!p->tag)
                        continue;
                    ++tagged;
                    f << p->id << ","
                      << helpers::escapeQuotes(p->user) << ","
                      << helpers::escapeQuotes(p->repo) << ","
                      << p->createdAt << std::endl;
                }
                std::cerr << "    " << tagged << " projects written" << std::endl;
                std::cerr << "    " << projects_.size() << "projects total" << std::endl;
            }

            /** First remove any untagged commits from the hierarchy. Then output the surviving ones.
             */
            void filterCommits() {
                std::cerr << "Removing empty commits..." << std::endl;
                unsigned removed = 0;
                for (auto i : commits_) {
                    Commit * c = i.second;
                    if (c->tag)
                        continue;
                    ++removed;
                    c->detach();
                }
                std::cerr << "    " << removed << " commits detached" << std::endl;
                std::cerr << "    " << commits_.size() << " out of total" << std::endl;
                {
                    std::cerr << "Writing filtered commits..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commits.csv");
                    f << "commitId,authorTime,committerTime" << std::endl;
                    for (auto i : commits_) {
                        Commit * c = i.second;
                        if (!c->tag)
                            continue;
                        f << c->id << "," << c->time << "," << c->time2 << std::endl;
                    }
                }
                {
                    std::cerr << "Writing commit parents..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/commitParents.csv");
                    f << "commitId,parentId" << std::endl;
                    for (auto i : commits_) {
                        Commit * c = i.second;
                        if (!c->tag)
                            continue;
                        for (Commit * p : c->parents)
                            f << c->id << "," << p->id << std::endl;
                    }
                }
            }

            void createSymlinks() {
                std::cerr << "Creating symlinks..." << std::endl;
                helpers::System(STR("ln -s " << DataDir.value() + "/hashes.csv " << OutputDir.value() << "/hashes.csv"));
                helpers::System(STR("ln -s " << DataDir.value() + "/paths.csv " << OutputDir.value() << "/paths.csv"));
            }
            
            

        private:


            void detectClonesInProject(Project * p) {
                CommitForwardIterator<Project, Commit, State> i(p, [&, this](Commit * c, State & state){
                        // first remove all files added by the commit
                        for (unsigned pathId : c->deletions) {
                            assert(state.files.find(pathId) != state.files.end());
                            //if (state.files.find(pathId) == state.files.end())
                            //    std::cout << p->id << "," << c->id << "," << pathId << std::endl;
                            state.files.erase(pathId);
                        }
                        // now for each change, update the change and determine if the thing is a clone
                        for (auto i : c->changes) {
                            // if the file has not been observed yet, it may be clone
                            if (state.files.find(i.first) == state.files.end()) {
                                // let's see if there is an original
                                auto o = originals_.find(i.second);
                                if (o != originals_.end()) {
                                    // ignore intra-project clones (this also ignores originals being clones)
                                    if (o->second->project != p) 
                                        p->clones.push_back(new FileClone(p->id, c->id, i.first, i.second));
                                }
                            }
                            // add the change
                            state.files[i.first] = i.second;
                        }
                        return true;
                    });
                i.process();
            }

            void analyzeOriginal(FileOriginal * o) {
                Project * p = o->project;
                for (Commit * c : p->commits) {
                    for (unsigned pathId : c->deletions)
                        if (pathId == o->fileId)
                            o->addContents(c, FILE_DELETED);
                    for (auto i : c->changes)
                        if (i.first == o->fileId)
                            o->addContents(c, i.second);
                }
                // now create the sorted list
                std::map<uint64_t, unsigned> sorted;
                for (auto i : o->contents)
                    sorted.insert(std::make_pair(i.second, i.first));
                for (auto i : sorted)
                    o->sortedContents.push_back(i);
                // and calculate all clones
                for (FileClone * c : o->clones)
                    analyzeClone(o, c);
            }

            void analyzeClone(FileOriginal * original, FileClone * clone) {
                std::map<uint64_t, unsigned> cloneSorted;
                Project * p = projects_[clone->projectId];
                Commit * start = commits_[clone->commitId];
                CommitForwardIterator<Project, Commit, BehaviorState> i(p, [&, this](Commit * c, BehaviorState & state){
                        if (state.active) {
                            // if the cloned file has been removed, no need to analyze the project further
                            if (c->deletions.find(clone->pathId) != c->deletions.end())
                                return false;
                            auto i = c->changes.find(clone->pathId);
                            if (i != c->changes.end()) {
                                cloneSorted.insert(std::make_pair(c->time, i->second));
                                ++clone->changingCommits;
                                if (clone->youngestChange < c->time)
                                    clone->youngestChange = c->time;
                                // see if the change is diverging or not
                                auto io = original->contents.find(i->second);
                                if (io == original->contents.end()) {
                                    ++clone->divergentCommits;
                                    if (clone->youngestDivergentChange < c->time)
                                        clone->youngestDivergentChange = c->time;
                                } else {
                                    uint64_t originalTime = io->second;
                                        if (originalTime <= c->time) {
                                            ++clone->syncCommits;
                                            clone->syncDelay += (c->time - originalTime);
                                            if (clone->youngestSyncChange < c->time)
                                                clone->youngestSyncChange = c->time;
                                        }
                                }
                            }
                            return true;
                        } else {
                            if (c == start) {
                                cloneSorted.insert(std::make_pair(c->time, clone->cloneId));
                                state.active = true;
                            }
                            return true;
                        }
                    });
                i.process();
                // now we must calculate the fully synced time
                size_t o = 0;
                uint64_t tmax = std::max(cloneSorted.rbegin()->first, original->sortedContents.back().first);
                for (auto c = cloneSorted.begin(), ce = cloneSorted.end(); c != ce;) {
                    while (o != original->sortedContents.size() - 1) {
                        if (original->sortedContents[o].first == c->first)
                            break;
                        if (original->sortedContents[o].first > c->first) {
                            --o;
                            break;
                        }
                        ++o;
                    }
                    if (c->second == original->sortedContents[o].second) {
                        uint64_t start = c->first;
                        uint64_t end = tmax;
                        if (++c != ce && c->first < end)
                                end = c->first;
                        if (o != original->sortedContents.size() - 1 && original->sortedContents[o + 1].first < end)
                            end = original->sortedContents[o + 1].first;
                        clone->fullySyncedTime += end - start;
                        ++clone->fullySyncedCommits;
                    } else {
                        ++c;
                    }
                }
            }
            
            std::mutex mCerr_;

            std::mutex mClonesOut_;
            std::ofstream clonesOut_;
            
            
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<unsigned, Commit *> commits_;
            std::unordered_map<unsigned, std::string> paths_;
            std::unordered_map<unsigned, FileOriginal *> originals_;

        };


        
    } // anonymous namespace

    void DetectFileClones(int argc, char * argv[]) {
        Threshold.updateDefaultValue(2);
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        helpers::EnsurePath(OutputDir.value());

        FileClonesDetector fcd;
        fcd.loadData();
        fcd.removeUniqueFiles();
        fcd.detectClones();
        fcd.calculateBehavior();
        fcd.writeOriginals();
        fcd.filterFileChanges();
        fcd.filterProjects();
        fcd.filterCommits();
        fcd.createSymlinks();
    }


    
} // namespace dejavu 
