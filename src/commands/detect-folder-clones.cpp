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


/** Folder Clone Detection


 */

namespace dejavu {

    namespace {

        class Detector;




        /** State of a project at given commit.

            Keeps track of files & folders active and determines when folde clone candidates are present in given commit.

            This is the state being tracked by the commit iterator. 
         */
        class ProjectState {
        public:

            // commit iterator requirements

            /** Creates an empty project state.
             */
            ProjectState():
                root_(nullptr) {
            }

            /** Creates a project state that is a copy of existing one.

                Merges with the other state, which has semantics identical to the copy constructor.
             */
            ProjectState(ProjectState const & other):
                root_(nullptr) {
                mergeWith(other, nullptr);
            }

            /** Merges with the other state, i.e. adds all files from the other state which are not already present.
             */
            void mergeWith(ProjectState const & other, Commit * c) {
                for (auto i : other.files_) {
                    if (files_.find(i.first) == files_.end()) {
                        File * f = addGlobalFile(i.second.file, nullptr);
                        files_.insert(std::make_pair(i.first, FileInfo(i.second.contents, f)));
                    }
                }
            }

            void updateWith(Commit * c, Detector * d, std::unordered_set<Dir*> & cloneCandidates);

            unsigned contentsOf(File * f) const {
                auto i = files_.find(f->pathId);
                assert(i != files_.end());
                return i->second.contents;
            }

            ~ProjectState() {
                delete root_;
            }
            
        private:
            struct FileInfo {
                unsigned contents;
                File * file;
                FileInfo(unsigned contents = 0, File * file = nullptr):
                    contents(contents),
                    file(file) {
                }
            };


            /** Adds given file to the project state.
             */
            void addFile(unsigned pathId, unsigned contentsId, Detector * d, std::unordered_set<Dir*> * createdDirs = nullptr);


            /** Given a file from the global tree, creates its copy in the project state.

                First makes recursively sure that all parent directories exist (adding newly created ones to the createdDirs vector if not null) and then adds the file to its parent directory and to the map of all files by path. 
             */
            File * addGlobalFile(File * globalFile, std::unordered_set<Dir*> * createdDirs) {
                Dir * parent = addGlobalDir(globalFile->parent, createdDirs);
                return new File(globalFile->pathId, globalFile->name, parent);
            }

            /** Makes sure that given global dir exists in the project state, creating it, or any of its parent dirs along the way.

                If the createdDirs argument is specified, any newly created directories will be added to it.
            */
            Dir * addGlobalDir(Dir * globalDir, std::unordered_set<Dir*> * & createdDirs) {
                // if the global directory is root, return our root, or create it if it does not exist
                if (globalDir->parent == nullptr) {
                    if (root_ == nullptr)
                        root_ = createDirectory(globalDir->name, nullptr, createdDirs);
                    else
                        preventSubfolderCreation(root_, createdDirs);
                    return root_;
                    
                }
                Dir * parent = addGlobalDir(globalDir->parent, createdDirs);
                auto i = parent->dirs.find(globalDir->name);
                if (i != parent->dirs.end()) {
                    preventSubfolderCreation(i->second, createdDirs);
                    return i->second;
                }
                return createDirectory(globalDir->name, parent, createdDirs);
            }

            void preventSubfolderCreation(Dir * d, std::unordered_set<Dir *> * & createdDirs) {
                if (createdDirs == nullptr)
                    return;
                if (createdDirs->find(d) != createdDirs->end())
                    createdDirs = nullptr;
            }
            
            /** Deletes the given file.

                If the file was the last file in a folder, deletes the folder as well (recursively, including the root)
             */
            void deleteFile(unsigned pathId) {
                File * f = files_[pathId].file;
                
                assert(f != nullptr);
                files_.erase(pathId);
                Dir * d = f->parent;
                delete f;
                while (d->empty()) {
                    Dir * p = d->parent;
                    delete d;
                    if (p == nullptr) {
                        assert(d == root_);
                        root_ = nullptr;
                        break;
                    } else {
                        d = p;
                    }
                }
            }

            Dir * createDirectory(unsigned name, Dir * localParent, std::unordered_set<Dir*> * & createdDirs) {
                Dir * result = new Dir(name, localParent);
                if (createdDirs != nullptr) {
                    createdDirs->insert(result);
                    // prevent subdirectories to be created
                    createdDirs = nullptr;
                }
                return result;
            }
            
            
            /** The root directory, can be null. 
             */
            Dir * root_;

            /** Maps files based on their path id to their contents and File object.
             */
            std::unordered_map<unsigned, FileInfo> files_;
        };

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
                clonesOut_ << "#projectId,commitId,cloneId,folder,files" << std::endl;

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
                        state.updateWith(c, this, cloneCandidates);
                        for (auto i : cloneCandidates)
                            processCloneCandidate(p, c, i, state);
                        //                        ccs += cloneCandidates.size();
                        cloneCandidates.clear();
                        return true;
                });
                i.process();
            }

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

            std::string calculateCloneStringFragment(Dir * d, ProjectState & state) {
                std::map<unsigned, std::string> parts;
                for (auto i : d->dirs)
                    parts.insert(std::make_pair(i.first, calculateCloneStringFragment(i.second, state)));
                for (auto i : d->files) {
                    parts.insert(std::make_pair(i.first, std::to_string(state.contentsOf(i.second))));
                }
                return createCloneStringFromParts(parts);
            }

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

        void ProjectState::updateWith(Commit * c, Detector * d, std::unordered_set<Dir*> & cloneCandidates) {
            // first delete all files the commit deletes
            for (auto i : c->deletions) 
                deleteFile(i);
            // now walk the changes and update the state
            for (auto i : c->changes) {
                auto j = files_.find(i.first);
                if (j != files_.end()) 
                    j->second.contents = i.second;
                else 
                    addFile(i.first, i.second, d, & cloneCandidates);
            }
        }

        
        void ProjectState::addFile(unsigned pathId, unsigned contentsId, Detector * d, std::unordered_set<Dir*> * createdDirs) {
            assert(contentsId != FILE_DELETED);
            auto i = files_.find(pathId);
            // if the file already exists, just update its contents
            if (i != files_.end()) {
                i->second.contents = contentsId;
            } else {
                File * globalFile = d->paths_[pathId];
                File *f  = addGlobalFile(globalFile, createdDirs);
                files_.insert(std::make_pair(pathId, FileInfo(contentsId, f)));
            }
        }

        
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
