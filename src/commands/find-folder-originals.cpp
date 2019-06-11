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

            struct ByTime {
                bool operator () (std::pair<Project *, uint64_t> const & first, std::pair<Project *, uint64_t> const & second) const {
                    if (first.second < second.second)
                        return true;
                    if (first.second == second.second)
                        return first.first->createdAt < second.first->createdAt;
                    return false;
                }
            };

            /** Returns the hints sorted by time (ascending).
             */
            std::set<std::pair<Project *, uint64_t>, ByTime> sort() {
                std::set<std::pair<Project *, uint64_t>, ByTime> result;
                for (auto i : hints_)
                    result.insert(i);
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
                pathSegments_.clearHelpers();
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
                FolderCloneLoader{DataDir.value() + "/clone_originals_candidates.csv", [this](unsigned id, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path){
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
                totalCandidates = 0;


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
                std::cerr << "    " << totalCandidates << " total candidates" << std::endl;
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



            void updateOriginal(Clone * c) {
                // build the clone dirs & files structure
                c->buildStructure(clones_);
                // get hints for projects which may contain the original
                LocationHint candidates = getCloneLocationHints(c);
                // sort the candidates by time
                unsigned vp = 0;
                unsigned vc = 0;
                for (auto i : candidates.sort()) {
                    // if the time at which the clone may appear in the project is younger than currently available clone, we can stop the search
                    if (i.second > c->commit->time)
                        break;
                    ++vp;
                    vc += checkForOriginalIn(i.first, c);
                }
                c->clearStructure();
                {
                    std::lock_guard<std::mutex> g(mData_);
                    candidateProjects_ += candidates.size();
                    visitedProjects_ += vp;
                }
            }

            
            unsigned checkForOriginalIn(Project * p, Clone * c) {

                return 0;    
            }
            
            std::atomic<unsigned long> totalCandidates;
                
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;
            std::vector<File *> paths_;
            PathSegments pathSegments_;
            Dir * globalRoot_;
            std::unordered_map<uint64_t, LocationHint> locationHints_;
            std::vector<Clone *> clones_;

            std::mutex mCerr_;
            std::mutex mData_;

            size_t candidateProjects_;
            size_t visitedProjects_;
            size_t visitedCommits_;
            

            
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
        
    }
    
} // namespace dejavu
