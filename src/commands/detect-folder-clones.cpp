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

    This is the batch mode folder clone detector. It loads all projects, commits and paths and builds one very large project tree structure which is an unification of all paths ever seen in the corpus.

    The folder detection algorithm itself then for each project (easy to parallelize) walks the commits in their topological order and for each commit reconstructs the actual project tree. If there is a folder found that contains at least given threshold of files which were all added by the commit analyzed, we call this folder to be a clone candidate.

    For each candidate the set of its file indices (filename id excluding the path and the contents id of the file) is created and all projects are checked against it so that we end up with a much smaller list of projects that we know contained, over their lifetime the files in the clone candidate.

    For each such project we then reconstruct its project tree commit by commit in topological order and whenever a file is added whose indice (filename id + contents id) exists in the clone candidate is added by commit, we check whether the project contains the clone candidate as its subset subtree. If it is we have found the possible original.

    Of course, only the oldest original candidate is the original so whenever the search for original goes to commit younger than current original, or an older original is found, search in current project terminates and another project from the original candidates will be tried. 

    


 */

namespace dejavu {

    namespace {

        class Detector;

        class Commit : public BaseCommit<Commit> {
        public:
            Commit(unsigned id, uint64_t time):
                BaseCommit<Commit>(id, time) {
            }
        };

        class Project : public BaseProject<Project, Commit> {
        public:
            Project(unsigned id, uint64_t createdAt):
                BaseProject<Project, Commit>(id, createdAt) {
            }

        };


        class Dir;
        
        class File {
        public:
            unsigned pathId;
            unsigned name;
            Dir * parent;
            
            File(unsigned pathId, unsigned name, Dir * parent);

            ~File();
        };

        
        class Dir {
        public:
            unsigned name;
            Dir * parent;
            std::unordered_map<unsigned, Dir *> dirs;
            std::unordered_map<unsigned, File *> files;


            size_t numFiles() const {
                size_t result = files.size();
                for (auto i : dirs) 
                    result += i.second->numFiles();
                return result;
            }

            std::string path(std::vector<std::string> const & idsToNames) const {
                if (parent == nullptr)
                    return "";
                std::string ppath = parent->path(idsToNames);
                if (ppath.empty())
                    return idsToNames[name];
                else
                    return ppath + "/" + idsToNames[name];
            }

            bool empty() const {
                return dirs.empty() && files.empty();
            }


            File * addPath(unsigned id, std::string const & path, Detector * detector);

            Dir(unsigned name, Dir * parent):
                name(name),
                parent(parent) {
                if (parent != nullptr) {
                    assert(parent->dirs.find(name) == parent->dirs.end());
                    parent->dirs.insert(std::make_pair(name, this));
                }
            }
            
            ~Dir() {
                if (parent != nullptr)
                    parent->dirs.erase(name);
                while (!dirs.empty())
                    delete dirs.begin()->second;
                while (!files.empty())
                    delete files.begin()->second;
            }

        private:

            File * createFile(unsigned pathId, unsigned name) {
                assert(files.find(name) == files.end());
                File * f = new File(pathId, name, this);
                files.insert(std::make_pair(name, f));
                return f;
            }
            
            Dir * getOrCreateDirectory(unsigned name) {
                auto i = dirs.find(name);
                if (i != dirs.end())
                    return i->second;
                return new Dir(name, this);
            }
        };



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

        class Clone {
        public:
            unsigned id;
            Project * project;
            Commit * commit;
            std::string dir;
            
            unsigned count;

            Clone(unsigned id, Project * p, Commit * c, std::string const & d):
                id(id),
                project(p),
                commit(c),
                dir(d),
                count(1) {
            }

            void updateWithOccurence(Project * p, Commit * c, std::string const & d) {
                // increase the count
                ++count;
                // now determine if this occurence is older and therefore should replace the original
                if ((c->time < commit->time) ||
                    (c->time == commit->time && p->createdAt < project->createdAt)) {
                    project = p;
                    commit = c;
                    dir = d;
                }
            }
        };


        class Detector {
        public:

            Detector() {
                pathSegments_.push_back("");
                pathSegmentsHelper_.insert(std::make_pair("",EMPTY_PATH));
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
                        paths_[id] = globalRoot_->addPath(id, path, this);
                    }};
                std::cerr << "    " << pathSegments_.size() << " unique path segments" << std::endl;
                pathSegmentsHelper_.clear();
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
                clonesOut_ << "#projectId,commitId,cloneId,folder" << std::endl;

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

                std::ofstream clones(DataDir.value() + "/clone_originals_candidates.csv");
                clones << "#cloneId,count,hash,projectId,commitId,path" << std::endl;
                for (auto i : clones_)
                    clones << i.second->id << "," << i.second->count << "," << i.first << "," << i.second->project->id << "," << i.second->commit->id << "," << helpers::escapeQuotes(i.second->dir) << std::endl;
            }

        private:
            friend class Project;
            friend class Dir;
            friend class ProjectState;



            unsigned getPathSegmentIndex(std::string name) {
                auto i = pathSegmentsHelper_.find(name);
                if (i == pathSegmentsHelper_.end()) {
                    unsigned id = pathSegments_.size();
                    pathSegments_.push_back(name);
                    i = pathSegmentsHelper_.insert(std::make_pair(name, id)).first;
                }
                return i->second;
            }
            

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
                // now determine if the dir itself is a clone candidate - this is true if at least one of its subdirs is a clone candidate, or if the number of files in it is greater or equal to the threshold
                if (subdirClones.empty() && cloneRoot->files.size() < Threshold.value() && cloneRoot->numFiles() < Threshold.value())
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
                        i = clones_.insert(std::make_pair(hash, new Clone(clones_.size(), p, c, path))).first;
                        outputString = true;
                    } else {
                        i->second->updateWithOccurence(p, c, path);
                    }
                    cloneId = i->second->id;
                }
                if (outputString) {
                    std::string x = STR(cloneId << "," << cloneString << "\n");
                    {
                        std::lock_guard<std::mutex> g(mCloneStrings_);
                        cloneStrings_ << x;
                    }
                }
                std::string x = STR(cloneId << "," << p->id << "," << c->id << "," << helpers::escapeQuotes(path) << "\n");
                {
                    std::lock_guard<std::mutex> g(mClonesOut_);
                    clonesOut_ << x;
                }
                return cloneString;
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
            std::vector<std::string> pathSegments_;
            std::unordered_map<std::string, unsigned> pathSegmentsHelper_;
            
            Dir * globalRoot_;

            std::unordered_map<SHA1Hash, Clone*> clones_;
            std::mutex mClones_;



            std::ofstream clonesOut_;
            std::mutex mClonesOut_;

            std::ofstream cloneStrings_;
            std::mutex mCloneStrings_;


            std::mutex mCerr_;
            
        }; 

        File::File(unsigned pathId, unsigned name, Dir * parent):
            pathId(pathId),
            name(name),
            parent(parent) {
            assert(parent != nullptr);
            assert(parent->files.find(name) == parent->files.end());
            parent->files.insert(std::make_pair(name, this));
        }

        
        File::~File() {
            if (parent != nullptr)
                parent->files.erase(name);
        }
        
        File * Dir::addPath(unsigned id, std::string const & path, Detector * detector) {
            assert(parent == nullptr && "files can only be added to root dirs");
            // split the path into the folders
            std::vector<std::string> p = helpers::Split(path, '/');
            Dir * d = this;
            for (size_t i = 0; i + 1 < p.size(); ++i) // for all directories
                d = d->getOrCreateDirectory(detector->getPathSegmentIndex(p[i]));
            return new File(id, detector->getPathSegmentIndex(p.back()), d);
        }

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
