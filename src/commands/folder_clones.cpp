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

        class CloneOriginal;

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


                /** Populates a map from file indices to directories containing them.  
                 */
                void getFileIndices(std::unordered_map<uint64_t, std::vector<Dir*>> & indices) {
                    for (auto i : dirs)
                        i.second->getFileIndices(indices);
                    for (auto i : files) {
                        uint64_t id = File::Get(i.first)->filename->id;
                        id = (id << 32) | i.second;
                        indices[id].push_back(this);
                    }
                }


                /** Determines if the current directory forms a subtree in the other project tree and returns the root of the subtree in the other project tree.

                 */
                Dir * isSubtreeOf(Dir * ownAnchor, Dir * otherAnchor) {
                    // using the anchors, determine the root candidate directory in the other tree (i.e. move back both anchors as many times as is required for own anchor to become the root dir - this)
                    while (ownAnchor != this) {
                        otherAnchor = otherAnchor->parent;
                        ownAnchor = ownAnchor->parent;
                        if (otherAnchor == nullptr)
                            return nullptr;
                    }
                    // now simply check whether ownAnchor is a subtree of otherAnchor
                    return ownAnchor->isSubtreeOf(otherAnchor) ? otherAnchor : nullptr;
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
                /** Determines whether given directory is a subset of the other dir.

                    A directory is subset if all files it contains are also present in the other dir with the same contents and if all its subdirs are subdirs of equally named directories in the otrher dir. 
                 */
                bool isSubtreeOf(Dir * other) {
                    if (files.size() < other->files.size())
                        return false;
                    if (dirs.size() < other->dirs.size())
                        return false;
                    for (auto i : files) {
                        auto j = other->files.find(i.first);
                        if (j == other->files.end())
                            return false;
                        if (i.second != j->second)
                            return false;
                    }
                    for (auto i : dirs) {
                        auto j = other->dirs.find(i.first);
                        if (j == other->dirs.end())
                            return false;
                        if (! i.second->isSubtreeOf(j->second))
                            return false;
                    }
                    return true;
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
                    Dir * d = getOrCreateDir(f->parent, nullptr);
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
                        updateFile(change.first, change.second, & cloneCandidates);
                }
            }

            struct DirPair {
                Dir * clone;
                Dir * originalCandidate;

                DirPair(Dir * clone, Dir * originalCandidate):
                    clone(clone),
                    originalCandidate(originalCandidate) {
                    }

                bool operator == (DirPair const & other) const {
                    return clone == other.clone && originalCandidate == other.originalCandidate;
                }
            };

            struct DirPairHash {
                size_t operator () (DirPair const & x) const {
                    std::hash<Dir *> h;
                    return h(x.clone) + h(x.originalCandidate);
                }

            };

            typedef std::unordered_set<DirPair, DirPairHash> OriginalCandidateSet;

            OriginalCandidateSet updateBy(Commit * commit, CloneOriginal & original);

        private:
            friend class Dir;

            void deleteFile(unsigned pathId) {
                if (files_.find(pathId) == files_.end()) {
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

            Dir * updateFile(unsigned pathId, unsigned hash, std::vector<Dir *> * cloneCandidates) {
                // if the file is not created new, but only updated, there is nothing to do, but change the 
                if (files_.find(pathId) != files_.end()) {
                    Dir * d = files_[pathId];
                    d->files[pathId] = hash;
                    return d;
                }
                // otherwise add the file and create any directories required for it, first of these will be a clone candidate
                File * f = File::Get(pathId);
                assert(f != nullptr);
                Dir * d = getOrCreateDir(f->parent, cloneCandidates);
                d->files[pathId] = hash;
                files_.insert(std::make_pair(pathId, d));
                return d;
            }

            Dir * getOrCreateDir(Directory * d, std::vector<Dir *> * cloneCandidates) {
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


        class Project;

        /** Describes the original or a candidate.
         */
        class CloneOriginal {
        public:
            unsigned id;
            Project * project;
            Commit * commit;
            std::string const & path;
            
            /** The clone candidate project subtree. 
             */
            ProjectTree::Dir * clone;

            /** Map from filename|contents hash to directories in the clone candidate above where the particular filename|contents can be found. 
             */
            std::unordered_map<uint64_t, std::vector<ProjectTree::Dir*>> files;

            CloneOriginal(unsigned id, Project * project, Commit * commit, std::string const & path, ProjectTree::Dir * clone):
                id(id),
                project(project),
                commit(commit),
                path(path),
                clone(clone) {
                clone->getFileIndices(files);
            }

            /** Returns a list of projects that could potentially contain the original of the clone. 
             */
            std::unordered_set<unsigned> getProjectCandidates();
                
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

            /** Updates the clone original if the project contains an older snapshot.
             */
            void findOriginal(CloneOriginal & original);
            
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

                // this needs to be guarded for multithreaded
                auto ci = CloneOriginals_.find(id);
                if (ci != CloneOriginals_.end()) {
                    // TODO output the project commit id and original info
                    return;
                }
                CloneOriginal original(CloneOriginals_.size() + 1, p, c, clone->path(), clone);
                CloneOriginals_.insert(std::make_pair(id, original.id));

                // now we must actually find the original, so first determine set of projects that are worth checking, which is projects which contain all of the files involved in the clone
                std::unordered_set<unsigned> projects(original.getProjectCandidates());


                // we have to examine this even if there is only one project because the project might copy from itself and the earlier instance was not a clone candidate itself
                std::cout << "Possible original projects reduced to " << projects.size() << std::endl;
                for (auto p : projects) 
                    Project::Get(p)->findOriginal(original);
                // TODO output the original
            }

            static std::unordered_set<unsigned> const & GetProjectsFor(uint64_t triageIndex) {
                auto i = ProjectsTriage_.find(triageIndex);
                assert(i != ProjectsTriage_.end());
                return i->second;
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

        // path + contentsId -> set of projects
        std::unordered_map<uint64_t, std::unordered_set<unsigned>> FolderCloneDetector::ProjectsTriage_;
        std::unordered_map<std::string, unsigned> FolderCloneDetector::CloneOriginals_;
        unsigned long FolderCloneDetector::NumClones_ = 0;



        ProjectTree::OriginalCandidateSet ProjectTree::updateBy(Commit * commit, CloneOriginal & original) {
            OriginalCandidateSet candidates;
            // delete files the same normal update works
            for (auto const & change : commit->changes)
                if (change.second == 0)
                    deleteFile(change.first);
            // now add files
            for (auto const & change : commit->changes) {
                if (change.second != 0) {
                    Dir * d = updateFile(change.first, change.second, nullptr);
                    // check if the file we just added is interesting, i.e. if its indice exists in the original
                    uint64_t x = File::Get(change.first)->filename->id;
                    x = (x << 32) | change.second;
                    auto i = original.files.find(x);
                    if (i != original.files.end()) {
                        for (auto j : i->second)
                            candidates.insert(DirPair(j,d));
                    } 
                }
            }
            return candidates;
        }



        /** Returns a list of projects that could potentially contain the original of the clone. 
         */
        std::unordered_set<unsigned> CloneOriginal::getProjectCandidates() {
            std::unordered_set<unsigned> projects;
            auto i = files.begin(), e = files.end();
            // add all projects for the first file indice
            for (auto j : FolderCloneDetector::GetProjectsFor(i->first))
                projects.insert(j);
            ++i;
            // now check the rest
            while (i != e) {
                auto & iprojects = FolderCloneDetector::GetProjectsFor(i->first);
                for (auto j = projects.begin(), je = projects.end(); j != je;) {
                    if (iprojects.find(*j) == iprojects.end())
                        j = projects.erase(j);
                    else 
                        ++j;
                }
                ++i;
            }
            // if the clones were empty it's an indication the triage is not working
            assert(! projects.empty());
            return projects;
        }


        /** First, we find the clone candidates.
         */
        void Project::detectFolderClones() {
            //std::cout << "Project " << id << ", num commits: " << commits.size() << std::endl;
            CommitForwardIterator<Commit,ProjectTree> it([this](Commit * c, ProjectTree & tree) {
                try {
                    std::vector<ProjectTree::Dir *> candidates;
                    tree.updateBy(c, candidates);
                    for (ProjectTree::Dir * d : candidates) {
                        size_t numFiles = d->numFiles();
                        //if the clone candidate passes the threshold, find its clone
                        if (numFiles >= Threshold.value())
                            FolderCloneDetector::FindOriginalFor(this, c,  d);
                        d->untaint();
                            
                            
                    }
                } catch (...) {
                    std::cout << "Commit id" << c->id << std::endl;
                    std::cout << "Project id" << id << std::endl;
                    assert(false);
                }
                return true;
            });
            for (auto i : commits)
                if (i->numParentCommits() == 0)
                    it.addInitialCommit(i);
            it.process();
        }

        void Project::findOriginal(CloneOriginal & original) {
            //std::cout << "Project " << id << ", num commits: " << commits.size() << std::endl;
            CommitForwardIterator<Commit,ProjectTree> it([this, & original](Commit * c, ProjectTree & tree) {
                if (c->time > original.commit->time)
                    return false;
                // otherwise update the project tree and get the list of candidates
                auto candidates = tree.updateBy(c, original);
                // we now have the list of directory comparisons we must do, if any of them turns out to be valid, we have a better original
                for (auto dp : candidates) {
                    ProjectTree::Dir * od = original.clone->isSubtreeOf(dp.clone, dp.originalCandidate);
                    if (od != nullptr) {
                        original.project = this;
                        original.commit = c;
                        original.path = od->path;
                        return false;
                    }
                }
                return true;
            });
            for (auto i : commits)
                if (i->numParentCommits() == 0)
                    it.addInitialCommit(i);
            it.process();
            // now we know there is the one and only original 
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
