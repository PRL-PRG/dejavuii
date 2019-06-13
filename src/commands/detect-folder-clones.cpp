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

#include "folder_clones.h"

namespace dejavu {

    namespace {

        /** Detects all folder clone candidates in the dataset.

            A clone candidate is a folder with at least 2 (the threshold can be changed) files which has been added by a single commit. If the folder contains other subfolders which themselves meet the criteria, they are considered clone candidates as well.

            The detector walks through all projects and commits and for each commit detect folders matching the criteria above. Each such folder is then analyzed:

            The folder structure is encoded in a string with the following structure:

            DIR := '(' [ id ':' STR { ',' id ':' STR }])

            STR := contentsId (for files)
                |= DIR (for directories)
                |= '#' cloneId (for subdirectories that are themselves clones)

            The `id` is a name id of the path segment and the ids are ordered (ascending), therefore the mapping from a directory to its structure is unambiguous.

            The structure strings are necessary for the second step where originals are searched and the clones themselves must be reconstitured. 
         */
        class Detector {
        public:

            Detector() {
            }

            /** Loads the initial data required for the clone detection.
             */
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
                globalRoot_ = new Dir(EMPTY_PATH, nullptr);
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        if (id >= paths_.size())
                            paths_.resize(id + 1);
                        paths_[id] = globalRoot_->addPath(id, path, pathSegments_);
                    }};
                std::cerr << "    " << pathSegments_.size() << " unique path segments" << std::endl;
                std::cerr << "Storing path segments ..." << std::endl;
                pathSegments_.save(DataDir.value() + "/path_segments.csv");
                pathSegments_.clearHelpers();
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                    }};
            }

            /** Looks for clone candidates in all loaded projects.
             */
            void detectCloneCandidates() {
                std::cerr << "Analyzing projects for clone candidates..." << std::endl;
                clonesOut_ = std::ofstream(DataDir.value() + "/clone_candidates.csv");
                clonesOut_ << "#cloneId,projectId,commitId,folder,files" << std::endl;

                cloneStrings_ = std::ofstream(DataDir.value() + "/clone_strings.csv");
                cloneStrings_ << "#cloneId,string" << std::endl;
                    
                std::vector<std::thread> threads;
                size_t completed = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([stride, & completed, this]() {
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(mCerr_);
                                if (completed == projects_.size())
                                    return;
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << " : " << completed << "    \r" << std::flush;
                            }
                            if (p == nullptr)
                                continue;
                            detectCloneCandidatesInProject(p);
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "Clone candidates: " << clones_.size() << std::endl;

                std::cerr << "Writing results..." << std::endl;

                std::ofstream clones(DataDir.value() + "/clone_originals_candidates.csv");
                clones << "#cloneId,hash,occurences,files,projectId,commitId,path" << std::endl;
                for (auto i : clones_)
                    clones << *(i.second) << std::endl;
            }

        private:
            friend class Project;
            friend class Dir;
            friend class ProjectState;

            

            /** Looks for all clone candidates in the given project.
             */
            void detectCloneCandidatesInProject(Project * p) {
                std::unordered_set<Dir*> cloneCandidates;
                CommitForwardIterator<Project,Commit,ProjectState> i(p, [&,this](Commit * c, ProjectState & state) {
                        // update the project state and determine the clone candidate folders
                        state.updateWith(c, paths_, & cloneCandidates);
                        for (auto i : cloneCandidates)
                            processCloneCandidate(p, c, i, state);
                        cloneCandidates.clear();
                        return true;
                });
                i.process();
            }

            /** Processes single clone candidate.

                The clone candidate is defined by its root directory and the string which encodes the clone candidate structure is returned.

                First all subdirs are processed as clone candidates and their structure is cached. After this, using the cached 
structures of the subdirs, remaining subdirs and files, the structure of the directory is created and then hashed.

                Then we check, based on the hash, whether such a clone has already been found and if not, create the clone and output its structure.

                Finally the number of occurences in the clone is bumped and the clone candidate is reported. 
             */
            std::string processCloneCandidate(Project * p, Commit * c, Dir * cloneRoot, ProjectState & state) {
                // first determine if any of the subdirs is a clone candidate itself and process it, returning its string
                std::map<unsigned, std::string> subdirClones;
                for (auto i : cloneRoot->dirs) {
                    std::string x = processCloneCandidate(p, c, i.second, state);
                    if (! x.empty())
                        subdirClones.insert(std::make_pair(i.first, std::move(x)));
                }
                unsigned numFiles = cloneRoot->numFiles();
                // now determine if the dir itself is a clone candidate
                if (numFiles < Threshold.value())
                    return "";
                // if the directory is a viable clone, create its string
                for (auto i : cloneRoot->files) 
                    subdirClones.insert(std::make_pair(i.first, std::to_string(state.contentsOf(i.second))));
                for (auto i : cloneRoot->dirs) 
                    if (subdirClones.find(i.first) == subdirClones.end())
                        subdirClones.insert(std::make_pair(i.first, calculateCloneStringFragment(i.second, state)));
                std::string cloneString = createCloneStringFromParts(subdirClones);
                // now that we have the string, create the hash of the clone, which we use for comparisons
                SHA1Hash hash;
                SHA1((unsigned char *) cloneString.c_str(), cloneString.size(), (unsigned char *) & hash.hash);
                std::string path = cloneRoot->path(pathSegments_);
                // see if the clone exists
                bool outputString = false;
                unsigned cloneId = 0;
                {
                    std::lock_guard<std::mutex> g(mClones_);
                    auto i = clones_.find(hash);
                    if (i == clones_.end()) {
                        i = clones_.insert(std::make_pair(hash, new Clone(clones_.size(), hash, p, c, path, numFiles))).first;
                        outputString = true;
                    } else {
                        i->second->updateWithOccurence(p, c, path, numFiles);
                    }
                    cloneId = i->second->id;
                }
                if (outputString) {
                    std::string x = STR(cloneId << ",\"" << cloneString << "\"\n");
                    {
                        std::lock_guard<std::mutex> g(mCloneStrings_);
                        cloneStrings_ << x;
                    }
                }
                std::string x = STR(cloneId << "," << p->id << "," << c->id << "," << helpers::escapeQuotes(path) << "," << numFiles << "\n");
                {
                    std::lock_guard<std::mutex> g(mClonesOut_);
                    clonesOut_ << x;
                }
                return STR("#" << cloneId);
            }

            /** Calculates the string representation of a given directory.
             */
            std::string calculateCloneStringFragment(Dir * d, ProjectState & state) {
                std::map<unsigned, std::string> parts;
                for (auto i : d->dirs)
                    parts.insert(std::make_pair(i.first, calculateCloneStringFragment(i.second, state)));
                for (auto i : d->files) {
                    parts.insert(std::make_pair(i.first, std::to_string(state.contentsOf(i.second))));
                }
                return createCloneStringFromParts(parts);
            }

            /** Creates string representation from fragments.
             */
            std::string createCloneStringFromParts(std::map<unsigned, std::string> const & parts) {
                std::stringstream result;
                assert(!parts.empty());
                auto i = parts.begin();
                result << "(" << i->first << ":" << i->second;
                ++i;
                for (auto e = parts.end(); i != e; ++i)
                    result << "," << i->first << ":" << i->second;
                result << ")";
                return result.str();
            }

            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;
            std::vector<File*> paths_;
            PathSegments pathSegments_;
            
            Dir * globalRoot_;

            std::unordered_map<SHA1Hash, Clone*> clones_;
            std::mutex mClones_;



            std::ofstream clonesOut_;
            std::mutex mClonesOut_;

            std::ofstream cloneStrings_;
            std::mutex mCloneStrings_;


            std::mutex mCerr_;
            
        }; 
        
    } // anonymous namespace



    void DetectFolderClones(int argc, char * argv[]) {
        Threshold.updateDefaultValue(2);
        NumThreads.updateDefaultValue(8);
        Settings.addOption(DataDir);
        Settings.addOption(Threshold);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        Detector d;
        d.loadData();
        d.detectCloneCandidates();
    }
    
} // namespace dejavu
