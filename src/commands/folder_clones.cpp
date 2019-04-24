#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>
#include <set>

#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {


        class Directory;


        class Filename {
        public:
            unsigned id;
            std::string name;
            std::unordered_set<unsigned> projects;

            static Filename * GetOrCreate(std::string const & name) {
                auto i = Filenames_.find(name);
                if (i == Filenames_.end())
                    i = Filenames_.insert(std::make_pair(name, new Filename(name))).first;
                return i->second;
            }

            static size_t Num() {
                return Filenames_.size();
            }
            
        private:

            Filename(std::string const & name):
                id(Filenames_.size()),
                name(name) {
            }

            static std::unordered_map<std::string, Filename *> Filenames_;
            
        };
        
        class File {
        public:
            Directory * parent;
            unsigned pathId;
            Filename * filename;


            static File * Create(Directory * parent, unsigned pathId, std::string const & filename) {
                File * result = new File(parent, pathId, filename);
                if (pathId >= Files_.size())
                    Files_.resize(pathId + 1);
                Files_[pathId] = result;
                return result;
                
            }

            static File * Get(unsigned pathId) {
                assert(pathId < Files_.size());
                return Files_[pathId];
            }
            
            static size_t Num() {
                return Files_.size();
            }

            
        private:
            
            File(Directory * parent, unsigned pathId, std::string const & filename):
                parent(parent),
                pathId(pathId),
                filename(Filename::GetOrCreate(filename)) {
            }

            static std::vector<File *> Files_;
            
        };

        class Directory {
        public:
            Directory * parent;
            std::string name;

            static Directory * Root() {
                return Root_;
            }

            static File * AddPath(unsigned id, std::string const & path) {
                std::vector<std::string> p = helpers::Split(path, '/');
                Directory * d = Root_;
                for (size_t i = 0; i + 1 < p.size(); ++i)  // for all directories
                    d = d->getOrCreateDir(p[i]);
                return d->createFile(id, p.back());
            }

        private:

            Directory(Directory * parent, std::string const & name):
                parent(parent),
                name(name) {
            }

            Directory * getOrCreateDir(std::string const & name) {
                auto i = dirs_.find(name);
                if (i == dirs_.end())
                    i = dirs_.insert(std::make_pair(name, new Directory(this, name))).first;
                return i->second;
            }

            File * createFile(unsigned pathId, std::string const & name) {
                assert(files_.find(name) == files_.end());
                File * f = File::Create(this, pathId, name);
                files_.insert(std::make_pair(name, f));
                return f;
            }
            
            std::unordered_map<std::string, Directory *> dirs_;
            std::unordered_map<std::string, File *> files_;

            static Directory * Root_;
        };


        class Commit {
        public:
            unsigned id;
            uint64_t time;
            unsigned numParents;
            std::unordered_map<unsigned, unsigned> changes;
            
            std::vector<Commit *> children;



            // implementation for the commit iterator
            std::vector<Commit *> const & childrenCommits() const {
                return children;
            }

            unsigned numParentCommits() const {
                return numParents;
            }

            static Commit * Create(unsigned id, uint64_t time) {
                assert(Commits_.find(id) == Commits_.end());
                Commit * result = new Commit(id, time);
                Commits_.insert(std::make_pair(id, result));
                return result;
            }

            static Commit * Get(unsigned id) {
                auto i = Commits_.find(id);
                assert(i != Commits_.end());
                return i->second;
            }

            
        private:

            Commit(unsigned id, uint64_t time):
                id(id), time(time), numParents(0) {}

        
            static std::unordered_map<unsigned, Commit *> Commits_;
        
        };

        /** Contains information about files in given project and commit.

         */
        class ProjectTree {
        public:
            class Dir {
            public:

                bool taint;
                
                /** Link to parent dir structure.
                 */
                Dir * parent;
                
                /** Link to the global tree structure for the given project directory.
                 */
                Directory * directory;

                /** Subdirectories - addressed by their global Directory objects.
                 */
                std::unordered_map<Directory *, Dir*> dirs;

                /** Files - addressed by their path ids, value is the hash of the file
                 */
                std::unordered_map<unsigned, unsigned> files;


                Dir(Dir * parent, Directory * directory):
                    taint(true),
                    parent(parent),
                    directory(directory) {
                }

                Dir(Dir * parent, Dir const * from):
                    taint(true),
                    parent(parent),
                    directory(from->directory) {
                    for (auto const & i : from->dirs)
                        dirs.insert(std::make_pair(i.first, new Dir(this, i.second)));
                    for (auto const & i : from->files)
                        files.insert(i);
                }

                ~Dir() {
                    for (auto i : dirs)
                        delete i.second;
                }

                bool isEmpty() const {
                    return dirs.empty() && files.empty();
                }

                void untaint() {
                    assert(taint == true);
                    taint = false;
                    for (auto i : dirs)
                        i.second->untaint();
                }

                size_t numFiles() const {
                    size_t result = 0;
                    for (auto i : dirs)
                        result += i.second->numFiles();
                    result += files.size();
                    return result;
                }

                std::string path() const {
                    if (parent == nullptr)
                        return "";
                    else
                        return parent->path() + "/" + directory->name;
                }

                /** Serializes the directory and its contents (recursively) into a single string.
                 */
                std::string serialize() {
                    std::stringstream s;
                    serialize(s);
                    return s.str();
                }
            private:
                void serialize(std::stringstream & s) {
                    s << "(";
                    // first get all subdirectories, order them by name and add them to the result
                    std::map<std::string, Directory *> d;
                    for (auto i : dirs)
                        d.insert(std::make_pair(i.first->name, i.first));
                    for (auto i : d)
                        s << i.first << ":" << dirs[i.second]->serialize() <<  ",";
                    // then get all files, order by name again and serialize them with the id of their contents
                    std::set<unsigned> f;
                    for (auto i : files)
                        f.insert(i.first);
                    for (auto i : f) 
                        s << i << ":" << files[i] << ",";
                    s << ")";
                }

            };

            ProjectTree():
                root_(nullptr) {
            }

            ProjectTree(ProjectTree const & from):
                root_(nullptr) {
                mergeWith(from);
                if (from.root_ != nullptr)
                    assert(root_ != nullptr);
                assert(dirs_.size() == from.dirs_.size());
                assert(files_.size() == from.files_.size());
            }

            ProjectTree(ProjectTree &&) = delete;

            ProjectTree & operator = (ProjectTree const &) = delete;
            ProjectTree & operator = (ProjectTree &&) = delete;

            ~ProjectTree() {
                delete root_;
            }

            /** Merges two states (i.e. branches) together. The merge retains all valid files from either of the branches.
             */
            void mergeWith(ProjectTree const & from) {
                for (auto i : from.files_) {
                    if (files_.find(i.first) != files_.end())
                        continue;
                    File * f = File::Get(i.first);
                    assert(f != nullptr);
                    Dir * d = getOrCreateDir(f->parent);
                    unsigned hash = i.second->files[i.first];
                    d->files.insert(std::make_pair(i.first, hash));
                    files_.insert(std::make_pair(i.first, d));
                }
            }

            /** Updates state with changes in given commit and returns 
             */
            void updateBy(Commit * commit, std::vector<Dir *> & cloneCandidates) {
                // firstdo all removes so that if commit deletes a folder and then creates it again, we will capture it
                for (auto const & change : commit->changes)
                    if (change.second == 0)
                        deleteFile(change.first);
                // then add all files in the commit
                for(auto const & change : commit->changes) {
                    if (change.second != 0)
                        updateFile(change.first, change.second, cloneCandidates);
                }
            }

        private:
            friend class Dir;

            void deleteFile(unsigned pathId) {
                if (files_.find(pathId) != files_.end()) {
                    std::cerr << "pathId: " << pathId << std::endl;
                    throw "HERE";
                }
                assert(files_.find(pathId) != files_.end());
                // get the file and its directory
                Dir * d = files_.find(pathId)->second;
                // erase the file from its directory
                d->files.erase(pathId);
                // erase the file from the list of files
                files_.erase(pathId);
                // while the directory contains no files, delete it
                while (d != nullptr && d->isEmpty()) {
                    Dir * x = d;
                    d = d->parent;
                    dirs_.erase(x->directory);
                    if (x == root_) 
                        root_ = nullptr;
                    else
                        d->dirs.erase(x->directory); // erase from parent
                    delete x;
                }
            }

            void updateFile(unsigned pathId, unsigned hash, std::vector<Dir *> & cloneCandidates) {
                // if the file is not created new, but only updated, there is nothing to do, but change the 
                if (files_.find(pathId) != files_.end()) {
                    files_[pathId]->files[pathId] = hash;
                    return;
                }
                // otherwise add the file and create any directories required for it, first of these will be a clone candidate
                File * f = File::Get(pathId);
                assert(f != nullptr);
                Dir * d = getOrCreateDir(f->parent, & cloneCandidates);
                d->files[pathId] = hash;
                files_.insert(std::make_pair(pathId, d));
            }

            Dir * getOrCreateDir(Directory * d, std::vector<Dir *> * cloneCandidates =nullptr) {
                assert(d != nullptr);
                auto i = dirs_.find(d);
                if (i != dirs_.end())
                    return i->second;
                if (d == Directory::Root()) {
                    root_ = new Dir(nullptr, d);
                    dirs_.insert(std::make_pair(d, root_));
                    if (cloneCandidates == nullptr)
                        root_->taint = false;
                    else
                        cloneCandidates->push_back(root_);
                    return root_;
                }
                assert(d->parent != nullptr);
                Dir * parent = getOrCreateDir(d->parent, cloneCandidates);
                Dir * result = new Dir(parent, d);
                dirs_.insert(std::make_pair(d, result));
                if (cloneCandidates == nullptr)
                    result->taint = false;
                else if (!parent->taint)
                    cloneCandidates->push_back(result);
                parent->dirs.insert(std::make_pair(d, result));
                return result;
            }
            
            Dir * root_;
            std::unordered_map<unsigned, Dir *> files_;
            std::unordered_map<Directory *, Dir *> dirs_;
        }; 


        /** The issue is that I have file A B C. a gets renamed to B and C gets renamed to A. Thus I see delete to A, and update to A in same commit, this needs to be purged.

            The join script can perhaps keep all purges? 
         */

        class Project {
        public:
            unsigned id;
            uint64_t createdAt;

            std::unordered_set<Commit *> commits;

            static Project * Create(unsigned id, uint64_t createdAt) {
                Project * result = new Project(id, createdAt);
                if (id >= Projects_.size())
                    Projects_.resize(id + 1);
                Projects_[id] = result;
                return result;
            }

            static Project * Get(unsigned id) {
                assert(id < Projects_.size());
                return Projects_[id];
            }

            static std::vector<Project *> const & GetAll() {
                return Projects_;
            }

            /** Detects folder clones in the given project.
             */
            void detectFolderClones();
            
        private:

            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(id) {
            }
            
            static std::vector<Project *> Projects_;
        };


        class FolderCloneDetector {
        public:
            FolderCloneDetector() {
                // load all projects
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project::Create(id, createdAt); 
                    }};
                // load all commits
                std::cerr << "Loading commits ... " << std::endl;
                CommitLoader{[](unsigned id, uint64_t authorTime, uint64_t committerTime){
                        Commit::Create(id, authorTime); 
                    }};
                //load commit parents
                std::cerr << "Loading commit parents ... " << std::endl;
                CommitParentsLoader{[](unsigned id, unsigned parentId){
                        Commit * c = Commit::Get(id);
                        Commit * p = Commit::Get(parentId);
                        ++c->numParents;
                        p->children.push_back(c);
                    }};
                
                // load all paths and populate the global tree
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[this](unsigned id, std::string const & path){
                        Directory::AddPath(id, path);
                    }};
                std::cerr << "    filenames: " << Filename::Num() << std::endl;
                std::cerr << "    files:     " << File::Num() << std::endl;
                // load all changes and fill in the commits & projects lists
                std::cerr << "Loading file changes ... " << std::endl;
                FileChangeLoader{[](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = Project::Get(projectId);
                        Commit * c = Commit::Get(commitId);
                        assert(p != nullptr);
                        assert(c != nullptr);
                        // add the commit to the project
                        p->commits.insert(c);
                        // add the change to the commit
                        c->changes.insert(std::make_pair(pathId, contentsId));
                        // remember that the project contains given file hash and filename
                        if (contentsId != 0)
                            AddToTriage(projectId, pathId, contentsId);
                    }};
                std::cerr << "    triage records: " << ProjectsTriage_.size() << std::endl;
            }

            /** Detects folder clones and their originals for all projects loaded.
             */
            static void Detect() {

                for (Project * p : Project::GetAll()) {
                    // skip any missing projects
                    if (p == nullptr)
                        continue;
                    p->detectFolderClones();
                }

                std::cout << "Number of clones:        " << NumClones_ << std::endl;
                std::cout << "Number of unique clones: " << CloneOriginals_.size() << std::endl;
            }

            /** Finds original for the given clone candidate represented as a directory in a project tree.
             */
            static void FindOriginalFor(Project * p, Commit * c, ProjectTree::Dir * clone) {
                ++NumClones_;
                std::string id = clone->serialize();
                auto i = CloneOriginals_.find(id);
                if (i != CloneOriginals_.end()) {
                    // TODO output the project commit id and original info
                    return;
                }
                // now we must actually find the original
                
                    
                CloneOriginals_.insert(std::make_pair(id, CloneOriginals_.size() + 1));
            }

        private:

            static void AddToTriage(unsigned projectId, unsigned pathId, unsigned contentsId) {
                assert(contentsId != 0);
                uint64_t x = File::Get(pathId)->filename->id;
                x = (x << 32) | contentsId;
                ProjectsTriage_[x].insert(projectId);
            }

            static std::unordered_map<uint64_t, std::unordered_set<unsigned>> ProjectsTriage_;

            static std::unordered_map<std::string, unsigned> CloneOriginals_;

            static unsigned long NumClones_;
        };

        std::unordered_map<std::string, Filename *> Filename::Filenames_;
        std::vector<File*> File::Files_;
        Directory * Directory::Root_ = new Directory(nullptr, "");

        std::unordered_map<unsigned, Commit *> Commit::Commits_;
        std::vector<Project *> Project::Projects_;

        std::unordered_map<uint64_t, std::unordered_set<unsigned>> FolderCloneDetector::ProjectsTriage_;
        std::unordered_map<std::string, unsigned> FolderCloneDetector::CloneOriginals_;
        unsigned long FolderCloneDetector::NumClones_ = 0;

        /** First, we find the clone candidates.
         */
        void Project::detectFolderClones() {
            //std::cout << "Project " << id << ", num commits: " << commits.size() << std::endl;
            CommitForwardIterator<Commit,ProjectTree> it([this](Commit * c, ProjectTree & tree) {
                    std::vector<ProjectTree::Dir *> candidates;
                    tree.updateBy(c, candidates);
                    for (ProjectTree::Dir * d : candidates) {
                        try {
                        size_t numFiles = d->numFiles();
                        //if the clone candidate passes the threshold, find its clone
                        if (numFiles >= Threshold.value())
                            FolderCloneDetector::FindOriginalFor(this, c,  d);
                        d->untaint();
                        } catch (...) {
                            std::cout << "Commit id" << c->id << std::endl;
                            std::cout << "Project id" << id << std::endl;
                            assert(false);
                        }
                    }
                    return true;
                });
            for (auto i : commits)
                if (i->numParentCommits() == 0)
                    it.addInitialCommit(i);
            it.process();
        }
        
    } // anonymous namespace

    void DetectFolderClones(int argc, char * argv[]) {
        Threshold.updateDefaultValue(2);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();
        FolderCloneDetector fcd;
        
        FolderCloneDetector::Detect();

        
    }
    
} // namespace dejavu
