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

#include "../commands.h"

#include "folder_clones.h"

/** Searches for originals of clone candidates.

    
 */

namespace dejavu {

    namespace {

        class LocationHint {
        public:

            LocationHint() {
                
            }

            LocationHint(LocationHint const & other):
                hints_(other.hints_) {
            }

            void cull(uint64_t minTime) {
                for (auto i = hints_.begin(), e = hints_.end(); i != e; ) {
                    if (i->second > minTime)
                        i = hints_.erase(i);
                    else
                        ++i;
                }
            }

            void join(LocationHint const & other, uint64_t minTime) {
                for (auto i = hints_.begin(), e = hints_.end(); i != e; ) {
                    auto j = other.hints_.find(i->first);
                    if (j == other.hints_.end()) {
                        i = hints_.erase(i);
                        continue;
                    } else {
                        if (j->second > minTime) {
                            i = hints_.erase(i);
                            continue;
                        }
                        if (j->second > i->second)
                            i->second = j->second;
                    }
                    ++i;
                }
            }
            /** Adds given commit and project to the location hints.

                If there is already a location hint for the given project, it is updated if the commit's time is older than the currently stored time.
             */
            void addOccurence(Project * p, Commit * c) {
                auto i = hints_.find(p);
                if (i == hints_.end()) 
                    hints_.insert(std::make_pair(p, c->time));
                 else 
                    if (i->second > c->time)
                        i->second = c->time;
            }

            size_t size() const {
                return hints_.size();
            }

            bool containsProject(Project * p) {
                return hints_.find(p) != hints_.end();
            }

            struct ByTime {
                bool operator () (std::pair<Project *, uint64_t> const & first, std::pair<Project *, uint64_t> const & second) const {
                    if (first.second != second.second)
                        return first.second < second.second;
                    if (first.first->createdAt != second.first->createdAt)
                        return first.first->createdAt < second.first->createdAt;
                    // compare pointers just to make sure that two commits of same age from projects of same age are not treated as identical
                    return first.first->id < second.first->id;
                }
            };

            /** Returns the hints sorted by time (ascending).
             */
            std::set<std::pair<Project *, uint64_t>, ByTime> sort() {
                std::set<std::pair<Project *, uint64_t>, ByTime> result;
                for (auto i : hints_)
                    result.insert(i);
                assert(result.size() == hints_.size());
                return result;
            }

        private:
            std::unordered_map<Project *, uint64_t> hints_;
            
        }; // FileLocationHint


        class OriginalFinder {
        public:
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        if (id >= projects_.size())
                            projects_.resize(id + 1);
                        projects_[id] = new Project(id, createdAt);
                    }};
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        if (id >= commits_.size())
                            commits_.resize(id + 1);
                        commits_[id] = new Commit(id, authorTime);
                    }};
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[this](unsigned id, unsigned parentId){
                        Commit * c = commits_[id];
                        Commit * p = commits_[parentId];
                        assert(c != nullptr);
                        assert(p != nullptr);
                        c->addParent(p);
                    }};
                std::cerr << "Loading path segments ... " << std::endl;
                pathSegments_.load();
                globalRoot_ = new Dir(EMPTY_PATH, nullptr);
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        if (id >= paths_.size())
                            paths_.resize(id + 1);
                        paths_[id] = globalRoot_->addPath(id, path, pathSegments_);
                    }};
                std::cerr << "Loading changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        getLocationHint(pathId, contentsId).addOccurence(p, c);
                    }};
                std::cerr << "    " << locationHints_.size() << " location hints" << std::endl;
                std::cerr << "    " << paths_.size() << " paths " << std::endl;
                std::cerr << "Loading clone candidates ..." << std::endl;
                FolderCloneOriginalsCandidateLoader{DataDir.value() + "/clone_originals_candidates.csv", [this](unsigned id, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path){
                        if (id >= clones_.size())
                            clones_.resize(id + 1);
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        clones_[id] = new Clone(id, hash, occurences, files, p, c, path);
                    }};
                std::cerr << "Loading clone structures ... " << std::endl;
                FolderCloneStructureLoader{[this](unsigned id, std::string const & str) {
                        Clone * c = clones_[id];
                        assert(c != nullptr);
                        c->str = str;
                    }};
            }

            void findOriginals() {
                std::cerr << "Updating clone originals..." << std::endl;

                std::vector<std::thread> threads;
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, this]() {
                        while (true) {
                            Clone * c;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == clones_.size())
                                    return;
                                c = clones_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (c == nullptr)
                                continue;
                            updateOriginal(c);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "    " << candidateProjects_ << " total candidates" << std::endl;
                std::cerr << "    " << visitedProjects_ << " actually checked projects" << std::endl;
                std::cerr << "    " << counts_.totalCommits << " total commits" << std::endl;
                std::cerr << "    " << counts_.visitedCommits << " visited commits" << std::endl;
                std::cerr << "    " << counts_.totalChanges << " observed changes" << std::endl;
                std::cerr << "    " << counts_.checkedChanges << " checked changes" << std::endl;
                std::cerr << "    " << counts_.totalDirs << " possible original roots" << std::endl;
                std::cerr << "    " << counts_.checkedDirs << " checked original roots" << std::endl;
                std::cerr << "    " << counts_.originalUpdates << " updates to clone originals" << std::endl;
            }

            /** The last step is to actually calculate the total number of files in the originals now that we have proper original locations.
                The easiest way to do this is to loop over all the projects once more, construct the project state (in full this time) and when we get a commit that is original of a clone, we determine the root directory and count its number of files.
             */
            void calculateFileCounts() {
                // first get for each commit the set of introduced clones
                std::cerr << "Attaching original clones to commits ..." << std::endl;
                for (Clone * c : clones_)
                    originals_[c->commit].insert(c);
                std::cerr << "Calculating clone original sizes ..." << std::endl;
                std::vector<std::thread> threads;
                size_t completed = 0;
                size_t skipped = 0;
                size_t updates = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, & skipped, & updates, this]() {
                        size_t changes = 0;
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size()) {
                                    updates += changes;
                                    return;
                                }
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            bool test = false;
                            for (Commit * c : p->commits)
                                if (originals_.find(c) != originals_.end()) {
                                    test = true;
                                    break;
                                }
                            if (test)
                                changes += calculateCloneSizesIn(p);
                            else
                                ++skipped;
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cout << "    " << skipped << " skipped projects" << std::endl;
                std::cout << "    " << updates << " updated clone sizes" << std::endl;
            }

            void output() {
                std::cerr << "Writing results..." << std::endl;
                std::ofstream clones(DataDir.value() + "/cloneOriginalsCandidates.csv");
                clones << "#cloneId,hash,occurences,files,projectId,commitId,path" << std::endl;
                for (auto i : clones_)
                    clones << *(i) << std::endl;
                std::cerr << "Done." << std::endl;
            }
        private:

            LocationHint & getLocationHint(unsigned path, unsigned contents) {
                static_assert(sizeof(unsigned) * 2 == sizeof(uint64_t), "Loss of data");

                unsigned filename = paths_[path]->name;
                uint64_t id = (static_cast<uint64_t>(filename) << (sizeof(unsigned) * 8)) + contents;
                return locationHints_[id];
            }

            LocationHint const & getLocationHint(File * f) {
                // NOTE that we repurpose f->pathId as contents id for the clone detector. 
                uint64_t id = (static_cast<uint64_t>(f->name) << (sizeof(unsigned) * 8)) + f->pathId;
                return locationHints_[id];
            }

            LocationHint getCloneLocationHints(Clone * c) {
                LocationHint candidates;
                bool join = false;
                getLocationHints(c->root, candidates, join, c->commit->time);
                assert(join == true);
                assert(candidates.size() >= 1);
                return candidates;
            }

            void getLocationHints(Dir * d, LocationHint & locations, bool & join, uint64_t minTime) {
                for (auto i : d->files) {
                    if (join) {
                        locations.join(getLocationHint(i.second), minTime);
                    } else {
                        locations = getLocationHint(i.second);
                        locations.cull(minTime);
                        join = true;
                    }
                }
                for (auto i : d->dirs)
                    getLocationHints(i.second, locations, join, minTime);
            }


            struct UpdateCounts {
                size_t totalCommits = 0;
                size_t visitedCommits = 0;
                size_t totalChanges = 0;
                size_t checkedChanges = 0;
                size_t totalDirs = 0;
                size_t checkedDirs = 0;
                size_t originalUpdates = 0;

                UpdateCounts & operator += (UpdateCounts const & other) {
                    totalCommits += other.totalCommits;
                    visitedCommits += other.visitedCommits;
                    totalChanges += other.totalChanges;
                    checkedChanges += other.checkedChanges;
                    totalDirs += other.totalDirs;
                    checkedDirs += other.checkedDirs;
                    originalUpdates += other.originalUpdates;
                    return *this;
                }
            };


            void updateOriginal(Clone * c) {
                // build the clone dirs & files structure
                c->buildStructure(clones_);
                // get hints for projects which may contain the original
                LocationHint candidates = getCloneLocationHints(c);
                assert(candidates.containsProject(c->project));
                // sort the candidates by time
                UpdateCounts counts;
                unsigned vp = 0;
                for (auto i : candidates.sort()) {
                    // if the time at which the clone may appear in the project is younger than currently available clone, we can stop the search
                    if (i.second > c->commit->time)
                        break;
                    // if the time is equal, but the project is youner, then it can't be original either
                    if (i.second == c->commit->time && i.first->createdAt >= c->project->createdAt)
                        break;
                    ++vp;
                    counts += checkForOriginalIn(i.first, c);
                }
                c->clearStructure();
                {
                    std::lock_guard<std::mutex> g(mData_);
                    candidateProjects_ += candidates.size();
                    visitedProjects_ += vp;
                    counts_ += counts;
                }
            }


            /** Given a project and clone, checks if the project contains a better clone original candidate and returns the number of commits tested.

             */
            UpdateCounts checkForOriginalIn(Project * p, Clone * clone) {
                UpdateCounts counts;
                counts.totalCommits += p->commits.size();
                std::unordered_set<File *> changedFiles;
                std::unordered_set<Dir*> candidates;
                CommitForwardIterator<Project, Commit, ProjectState> cfi(p, [&, this](Commit * c, ProjectState & state) {
                        // if we are past the so far original, stop
                        if (c->time > clone->commit->time)
                            return false;
                        // if we are the same time as the original, but the project is youner, stop
                        if (c->time == clone->commit->time && p->createdAt >= clone->project->createdAt)
                            return false;
                        // no need to deal with the original clone commit already
                        if (c == clone->commit)
                            return false;
                        ++counts.visitedCommits;
                        state.updateWith(c, paths_, clone->validContents, changedFiles);
                        counts.totalChanges += c->changes.size();
                        counts.checkedChanges += changedFiles.size();
                        // for each file that has changed
                        for (File * f : changedFiles) {
                            // get all files in the clone with the same contents and see if there is a matching directory in project state that can be root
                            for (File * cf : clone->validContents[state.contentsOf(f)]) { 
                                Dir * rootCandidate = findPossibleRoot(f, cf);
                                if (rootCandidate != nullptr)
                                    candidates.insert(rootCandidate);
                            }
                        }
                        changedFiles.clear();
                        counts.totalDirs += candidates.size();
                        // for all candidate folders, see if they are identical
                        if (!candidates.empty()) {
                            bool updated = false;
                            for (Dir * d : candidates) {
                                ++counts.checkedDirs;
                                if (isSubsetOf(clone->root, d, state)) {
                                    // to be deterministic, we do not optimize and we check all folders candidates taking the one with lexicographically smallest path
                                    std::string path = d->path(pathSegments_);
                                    if (clone->updateWithOccurence(p, c, path, clone->files)) {
                                        ++counts.originalUpdates;
                                        updated = true;
                                    }
                                }
                            }
                            candidates.clear();
                            if (updated)
                                return false;
                        }
                        return true;
                    });
                cfi.process();
                return counts;
            }

            /** Takes two files, and returns a directory from the first file's path that might be mapped to the root directory of the second file.
             */
            Dir * findPossibleRoot(File * f, File * cf) {
                if (f->name != cf->name)
                    return nullptr;
                Dir * d = f->parent;
                Dir * cd = cf->parent;
                while (d != nullptr) {
                    if (cd->parent == nullptr)
                        return d;
                    if (d->name != cd->name)
                        return nullptr;
                    d = d->parent;
                    cd = cd->parent;
                }
                return nullptr;
            }

            /** Returns true if the first directory is a subset of the second one.

                A directory is a subset if all files in the directory are present in the other directory and have identical contents and if all its directories are subsets of equally named directories in the other dir.

                The clone directory has contents stored in the pathId of the files, while the second directory's contents is to be obtained from the passed state. 
             */
            bool isSubsetOf(Dir * cd, Dir * d, ProjectState const & state) {
                for (auto i : cd->files) {
                    auto j = d->files.find(i.first);
                    if (j == d->files.end())
                        return false;
                    if (i.second->pathId != state.contentsOf(j->second))
                        return false;
                }
                for (auto i : cd->dirs) {
                    auto j = d->dirs.find(i.first);
                    if (j == d->dirs.end())
                        return false;
                    if (! isSubsetOf(i.second, j->second, state))
                        return false;
                }
                return true;
            }

            unsigned calculateCloneSizesIn(Project * p) {
                unsigned changes = 0;
                CommitForwardIterator<Project,Commit,ProjectState> i(p, [&,this](Commit * c, ProjectState & state) {
                        // update the project state and determine the clone candidate folders
                        state.updateWith(c, paths_, nullptr);
                        auto i = originals_.find(c);
                        if (i != originals_.end()) {
                            for (Clone * clone : i->second) {
                                if (clone->project == p) {
                                    assert(clone->commit == c);
                                    unsigned numFiles = state.getDir(clone->path, pathSegments_)->numFiles();
                                    assert(clone->files <= numFiles);
                                    if (numFiles > clone->files) {
                                        ++changes;
                                        clone->files = numFiles;
                                    }
                                }
                            }
                        }
                        return true;
                });
                i.process();
                return changes;
            }
            
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;
            std::vector<File *> paths_;
            PathSegments pathSegments_;
            Dir * globalRoot_;
            std::unordered_map<uint64_t, LocationHint> locationHints_;
            std::vector<Clone *> clones_;
            std::unordered_map<Commit *, std::unordered_set<Clone *>> originals_;

            std::mutex mCerr_;
            std::mutex mData_;

            size_t candidateProjects_ = 0;
            size_t visitedProjects_ = 0;
            UpdateCounts counts_;
        }; // OriginalFinder
        
    } // anonymous namespace


    void FindFolderOriginals(int argc, char * argv[]) {
        NumThreads.updateDefaultValue(8);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        OriginalFinder f;
        f.loadData();
        f.findOriginals();
        f.calculateFileCounts();
        f.output();
        
    }
    
} // namespace dejavu
