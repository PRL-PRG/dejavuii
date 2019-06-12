#pragma once

#include "../objects.h"
#include "../loaders.h"
#include "../commit_iterator.h"

namespace dejavu {

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

    class PathSegments {
    public:
        unsigned getIndex(std::string name) {
            auto i = helper_.find(name);
            if (i == helper_.end()) {
                unsigned id = pathSegments_.size();
                pathSegments_.push_back(name);
                i = helper_.insert(std::make_pair(name, id)).first;
            }
            return i->second;
        }

        void save(std::string const & filename) {
            std::ofstream psegs(filename);
            psegs << "#segmentId,str" << std::endl;
            for (size_t i = 0, e = pathSegments_.size(); i < e; ++i)
                psegs << i << "," << helpers::escapeQuotes(pathSegments_[i]) << std::endl;
        }

        void load() {
            PathSegmentsLoader{[this](unsigned id, std::string const & str) {
                    if (id >= pathSegments_.size())
                        pathSegments_.resize(id + 1);
                    pathSegments_[id] = str;
                    helper_.insert(std::make_pair(str, id));
                }};
        }

        void clearHelpers() {
            helper_.clear();
        }

        size_t size() const {
            return pathSegments_.size();
        }

        std::string const & operator [] (size_t index) const {
            assert(index < pathSegments_.size());
            return pathSegments_[index];
        }

        PathSegments() {
            pathSegments_.push_back("");
            helper_.insert(std::make_pair("",EMPTY_PATH));
        }
    private:
        std::vector<std::string> pathSegments_;
        std::unordered_map<std::string, unsigned> helper_;
    };

    /** Describes a file in either the global tree, or project level tree.

        Contains pathId of the path the file represents and its name.

        Note that folder clone original finder repurposes the pathId as contents id.

        TODO or not ??
     */
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

        std::string path(PathSegments const & pathSegments) const {
            if (parent == nullptr)
                return "";
            std::string ppath = parent->path(pathSegments);
            if (ppath.empty())
                return pathSegments[name];
            else
                return ppath + "/" + pathSegments[name];
        }

        bool empty() const {
            return dirs.empty() && files.empty();
        }


        File * addPath(unsigned id, std::string const & path, PathSegments & pathSegments) {
            assert(parent == nullptr && "files can only be added to root dirs");
            // split the path into the folders
            std::vector<std::string> p = helpers::Split(path, '/');
            Dir * d = this;
            for (size_t i = 0; i + 1 < p.size(); ++i) // for all directories
                d = d->getOrCreateDirectory(pathSegments.getIndex(p[i]));
            return new File(id, pathSegments.getIndex(p.back()), d);
        }

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

    inline File::File(unsigned pathId, unsigned name, Dir * parent):
        pathId(pathId),
        name(name),
        parent(parent) {
        assert(parent != nullptr);
        assert(parent->files.find(name) == parent->files.end());
        parent->files.insert(std::make_pair(name, this));
    }

        
    inline File::~File() {
        if (parent != nullptr)
            parent->files.erase(name);
    }

    /** Clone information.
     */
    class Clone {
    public:
        unsigned id;
        SHA1Hash hash;
        unsigned occurences;
        unsigned files;
        Project * project;
        Commit * commit;
        std::string path;
        std::string str;

        Dir * root;

        // contentsId -> set of Files in the clone with that contents
        std::unordered_map<unsigned, std::unordered_set<File *>> validContents;


        /** Creates the clone as part of the clone finder with incomplete data.

         */
        Clone(unsigned id, SHA1Hash const & hash, Project * p, Commit * c, std::string const & d, unsigned files):
            id(id),
            hash(hash),
            occurences(1),
            files(files),
            project(p),
            commit(c),
            path(d),
            root(nullptr) {
        }

        
        /** Creates the clone from previously stored data (i.e. in the originals finder).
         */
        Clone(unsigned id, SHA1Hash const & hash,unsigned occurences, unsigned files, Project * project, Commit * commit, std::string const & path):
            id(id),
            hash(hash),
            occurences(occurences),
            files(files),
            project(project),
            commit(commit),
            path(path),
            root(nullptr) {
        }

        /** Increases the number of clone occurences and updates the original info (the oldest clone occurence is consider an original at this stage).
         */
        void updateWithOccurence(Project * p, Commit * c, std::string const & d, unsigned files) {
            // increase the count
            ++occurences;
            // now determine if this occurence is older and therefore should replace the original
            if ((c->time < commit->time) ||
                (c->time == commit->time && p->createdAt < project->createdAt)) {
                project = p;
                commit = c;
                path = d;
                files = files;
            }
        }
        
        /** Builds the directory structure of the clone.

            Takes a vector of all clones as argument, since the clone may contain other clones recursively, which is where we get their contents from.

            Also populates the map from contents to files
         */
        void buildStructure(std::vector<Clone *> const & clones) {
            root = new Dir(EMPTY_PATH, nullptr);
            char const * x = str.c_str();
            fillDir(root, x, clones);
            assert(*x == 0);
            //fill the file contents map
            fillFilesFrom(root);
        }

        void clearStructure() {
            delete root;
            root = nullptr;
            validContents.clear();
        }

        ~Clone() {
            clearStructure();
        }

    private:

        friend std::ostream & operator <<(std::ostream & s, Clone const & c) {
            s << c.id << "," << c.hash << "," << c.occurences << "," << c.files << "," << c.project->id << "," << c.commit->id << "," << helpers::escapeQuotes(c.path);
            return s;
        }

        void pop(char const * & x, char what) {
            assert(*x == what);
            ++x;
        }

        unsigned getNumber(char const * & x) {
            unsigned result = 0;
            assert(*x >= '0' && *x <= '9');
            do {
                result = result * 10 + (*x++ - '0');
            } while (*x >= '0' && *x <='9');
            return result;
        }

        void fillDir(Dir * d, char const * & x, std::vector<Clone *> const & clones) {
            pop(x, '(');
            while (true) {
                unsigned nameId = getNumber(x);
                pop(x, ':');
                if (*x == '(') {
                    Dir * dd = new Dir(nameId, d);
                    fillDir(dd, x, clones);
                } else if (*x == '#') {
                    ++x;
                    unsigned cloneId = getNumber(x);
                    Dir * dd = new Dir(nameId, d);
                    char const * xx = clones[cloneId]->str.c_str();
                    fillDir(dd, xx, clones);
                } else {
                    unsigned contentsId = getNumber(x);
                    new File(contentsId, nameId, d);
                }
                if (*x == ',')
                    ++x;
                else break;
            }
            pop(x, ')');
        }

        void fillFilesFrom(Dir * d) {
            for (auto i : d->files)
                validContents[i.second->pathId].insert(i.second); // remember pathId is contents id
            for (auto i : d->dirs)
                fillFilesFrom(i.second);
        }
           
    }; // Clone


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

            void updateWith(Commit * c, std::vector<File*> const & paths, std::unordered_set<Dir*> * cloneCandidates) {
                // first delete all files the commit deletes
                for (auto i : c->deletions) 
                    deleteFile(i);
                // now walk the changes and update the state
                for (auto i : c->changes) {
                    auto j = files_.find(i.first);
                    if (j != files_.end()) 
                        j->second.contents = i.second;
                    else 
                        addFile(i.first, i.second, paths, cloneCandidates);
                }
            }

            void updateWith(Commit * c, std::vector<File*> const & paths, std::unordered_map<unsigned, std::unordered_set<File *>> const & validContents, std::unordered_set<File *> & changes) {
                // first delete all files the commit deletes
                for (auto i : c->deletions) {
                    if (files_.find(i) == files_.end())
                        continue;
                    deleteFile(i);
                }
                // now walk the changes and update the state
                for (auto i : c->changes) {
                    // if the update is to invalid contents then either ignore it, or delete the file if the file existed since it is no longer interesting
                    if (validContents.find(i.second) == validContents.end()) {
                        if (files_.find(i.first) != files_.end())
                            deleteFile(i.first);
                        continue;
                    }
                    auto j = files_.find(i.first);
                    File * f = nullptr;
                    if (j != files_.end()) {
                        f = j->second.file;
                        j->second.contents = i.second;
                    } else {
                        f = addFile(i.first, i.second, paths, nullptr);
                    }
                    assert(f != nullptr);
                    changes.insert(f);
                }
            }

            unsigned contentsOf(File * f) const {
                auto i = files_.find(f->pathId);
                assert(i != files_.end());
                return i->second.contents;
            }

            /** Returns directory corresponding to the given path.
             */
            Dir * getDir(std::string const & path, PathSegments & segments) {
                std::vector<std::string> psegs = helpers::Split(path, '/');
                Dir * d = root_;
                for (std::string const & name : psegs) {
                    unsigned nameId = segments.getIndex(name);
                    auto i = d->dirs.find(nameId);
                    assert(i != d->dirs.end());
                    d = i->second;
                }
                return d;
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
            File * addFile(unsigned pathId, unsigned contentsId, std::vector<File *> const & paths, std::unordered_set<Dir*> * createdDirs = nullptr) {
                assert(contentsId != FILE_DELETED);
                auto i = files_.find(pathId);
                // if the file already exists, just update its contents
                if (i != files_.end()) {
                    i->second.contents = contentsId;
                    return i->second.file;
                } else {
                    File * globalFile = paths[pathId];
                    File *f  = addGlobalFile(globalFile, createdDirs);
                    files_.insert(std::make_pair(pathId, FileInfo(contentsId, f)));
                    return f;
                }
            }


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
    



    

    
    
}
