#pragma once

#include <mutex>
#include <unordered_map>
#include <unordered_set>

/** Global tree

 */
namespace dejavu {

    namespace clones {
        constexpr unsigned MIN_CLONE_FILES = 2;

        class Filename;
        class CloneCandidate;

        class Hash {
        public:
            static std::unordered_set<unsigned> const & GetFor(unsigned id) {
                assert(id < hashes_.size());
                return hashes_[id];
            }

            static void AddProject(unsigned hashId, unsigned projectId) {
                if (hashId >= hashes_.size())
                    hashes_.resize(hashId + 1);
                hashes_[hashId].insert(projectId);
            }
            
        private:
            static std::vector<std::unordered_set<unsigned>> hashes_;
        }; 

        /** Commit representation for cloning purposes.

            Commit remembers its id, time it was created, origional projects and a changeset, i.e. map from path to hash id of files the commit changes.
         */
        class Commit {
        public:
            unsigned id;
            uint64_t time;
            unsigned originalProject;
            std::unordered_map<unsigned, unsigned> changes;

            static Commit * Create(unsigned id, uint64_t time, unsigned originalProject) {
                // TODO add checks that it does not exist yet for robustness
                return new Commit(id, time, originalProject);
            }

            static Commit * Get(unsigned id) {
                assert(id < commits_.size());
                return commits_[id];
            }

            static void Compact() {
                commits_.shrink_to_fit();
            }

            void addChange(unsigned pathId, unsigned hash) {
                changes.insert(std::make_pair(pathId, hash));
            }

            class ByTimeComparator {
            public:
                bool operator() (Commit * first, Commit * second) {
                    assert(first != nullptr && second != nullptr && "We don't expect null commit");
                    if (first->time != second->time)
                        return first->time < second->time;
                    else
                        return first < second; 
                }
            }; 

        private:
            Commit(unsigned id, uint64_t time, unsigned originalProject):
                id(id),
                time(time),
                originalProject(originalProject) {
                if (id >= commits_.size())
                    commits_.resize(id + 1);
                commits_[id] = this;
            }

            static std::vector<Commit*> commits_;
            
        }; // clones::Commit

        class CloneCandidate;
        
        /** Identifies a clone original.
         */
        class CloneOriginal {
        public:
            unsigned id;
            unsigned projectId;
            unsigned commitId;
            uint64_t time;
            unsigned numOccurences;
            std::string directory;

            /** Verifies that the original is valid for the current clone candidate.

                TODO I am not using this at all for now, will be used to double check that the hashes did not collide. 
             */
            bool verify(CloneCandidate const & cc);

            static unsigned GetFor(CloneCandidate * cc);

            static size_t NumOriginals() {
                std::lock_guard<std::mutex> g(m_);
                return originals_.size();
            }

            static void SaveAll(std::string const & where);

        private:
            CloneOriginal(unsigned id, CloneCandidate * cc);

            friend std::ostream & operator << (std::ostream & s, CloneOriginal const & co) {
                s << co. id << "," << co. projectId << "," << co. commitId << "," << co.time << "," << co.numOccurences << "," << helpers::escapeQuotes(co.directory);
                return s;
            }

            static std::mutex m_;
            static std::unordered_map<std::string, CloneOriginal *> originals_;
        };
        
        /** Representation of a project for cloning purposes.

            The project remembers is id, when it was created and all its commits ordered by time from oldest to youngest.
         */
        class Project {
        public:
            unsigned id;
            uint64_t createdAt;

            std::set<Commit *, Commit::ByTimeComparator> commits;

            static std::vector<Project*> const &  All() {
                return projects_;
            }

            static Project * Create(unsigned id, uint64_t createdAt) {
                // TODO add checks that it does not exist yet for robustness
                return new Project(id, createdAt);
            }

            static Project * Get(unsigned id) {
                assert(id < projects_.size());
                return projects_[id];
            }

            static size_t NumProjects() {
                return projects_.size();
            }

            static void Compact() {
                projects_.shrink_to_fit();
            }

            void addCommit(Commit * c) {
                commits.insert(c);
            }

            /** Creates a list of clone candidates for the project.
             */
            std::vector<CloneCandidate *> getCloneCandidates();

            /** Searches the project for possible older occurence of the clone candidate.
             */
            void updateCloneOriginal(CloneCandidate * cc, CloneOriginal & co, std::unordered_set<Filename *> filenames);

            class CreatedAtOrderer {
            public:
                bool operator() (Project * first, Project * second) const {
                    if (first->createdAt != second->createdAt)
                        return first->createdAt < second->createdAt;
                    else
                        return first < second;
                }
            };


        private:

            Project(unsigned id, uint64_t createdAt):
                id(id),
                createdAt(createdAt) {
                if (id > projects_.size())
                    projects_.resize(id + 1);
                projects_[id] = this;
            }

            

            static std::vector<Project*> projects_;
            
        }; // clones::Project
        

        /** Filename record.

            A record for each unique filename (discarding ist path). Contains the name itself and the projects it is contained in - this information is used as a triage for quickly selecting projects that might contain originals of the folders.
         */
        class Filename {
        public:
            /** Name of the file (excluding path)
             */
            std::string name;
            
            /** Set of projects where the filename occurs.
             */
            std::unordered_set<unsigned> projects;

            static Filename * GetOrCreate(std::string const & name) {
                auto i = filenames_.find(name);
                if (i == filenames_.end())
                    i = filenames_.insert(std::make_pair(name, new Filename(name))).first;
                return i->second;
            }

            static size_t Count() {
                return filenames_.size();
            }

            void addProject(Project * p) {
                projects.insert(p->id);
            }

        private:

            Filename(std::string const & name):
                name(name) {
            }

            static std::unordered_map<std::string, Filename *> filenames_;
            
        }; // clones::Filename

        class Directory;

        /** File record.

            This is the leaf of the unified root structure. It contains the path id, pointer to the filename (which has the name of the file and projects containing such filename) and the parent directory. 
         */
        class File {
        public:
            /** Directory to which the file belongs.
             */
            Directory * parent;

            /** id of the path.
             */
            unsigned pathId;

            /** Corresponding filename, which aggregates files with identical filenames across all projects.
             */
            Filename * filename;

            

            /** Returns the path of the file as string.
             */
            std::string getPath();

            /** Returns the File object corresponding to the given path id.
             */
            static File * Get(size_t pathId) {
                return files_[pathId];
            }

            /** Returns the total number of File objects stored.
             */
            static size_t Count() {
                return files_.size();
            }

            /** Compacts the internal storage.

                If more space was reserved than we have file objects, deallocates the extra space.
             */
            static void Compact() {
                files_.shrink_to_fit();
            }
            
        private:
            friend class Directory;

            /** Creates the new file, sets its properties and adds it to the list of known files.
             */
            File(Directory * parent, unsigned pathId, std::string const & filename):
                parent(parent),
                pathId(pathId),
                filename(Filename::GetOrCreate(filename)) {
                if (pathId >= files_.size())
                    files_.resize(pathId + 1);
                files_[pathId] = this;
            }

            static std::vector<File *> files_;
        }; // clones::File

        /** Directory record.

            Directory record in the unified root structure of all possible files contained in the projects we analyze. 
         */
        class Directory {
        public:
            std::string name;
            Directory * parent;
            std::unordered_map<std::string, Directory *> subdirs;
            std::unordered_set<File *> files;

            File * addPath(unsigned pathId, std::string const & path) {
                std::vector<std::string> p = helpers::split(path, '/');
                return addPath(pathId, p, 0);
            }

            /** Returns the path of the directory.
             */
            std::string getPath();

            static Directory * Root() {
                return & root_;
            }

            static size_t Count() {
                return numDirs_;
            }

        private:

            Directory(std::string const & name, Directory * parent):
                name(name),
                parent(parent) {
                ++numDirs_;
            }

            Directory * getOrCreateDirectory(std::string const & name) {
                auto i = subdirs.find(name);
                if (i == subdirs.end())
                    i = subdirs.insert(std::make_pair(name, new Directory(name, this))).first;
                return i->second;
            }
            
            File * addPath(unsigned pathId, std::vector<std::string> const & path, size_t index) {
                if (index == path.size() - 1) { // it's a file
                    File * result = new File(this, pathId, path[index]);
                    files.insert(result);
                    return result;
                } else {
                    return getOrCreateDirectory(path[index])->addPath(pathId, path, index + 1);
                }
            }

            static Directory root_;
            static size_t numDirs_;
        }; // clones::Directory

        class ProjectDir;

        /** File information inside given project.
         */
        class ProjectFile {
        public:
            ProjectDir * parent;
            unsigned pathId;
            unsigned hashId;
            unsigned addedByCommit;

            ProjectFile(ProjectDir * parent, unsigned pathId):
                parent(parent),
                pathId(pathId),
                hashId(0),
                addedByCommit(0) {
            }

            ProjectFile(ProjectDir * parent, ProjectFile const * from):
                parent(parent),
                pathId(from->pathId),
                hashId(from->hashId),
                addedByCommit(from->addedByCommit) {
            }

            void update(unsigned commit, unsigned hash) {
                if (hashId == 0) {
                    // TODO only enable this when I have the proper commit histories from konrad as now there is no ordering of commits happening at the same time
                    //assert(hash != 0);
                    addedByCommit = commit;
                }
                hashId = hash;
            }

            /** Determines the root directory for a clone candidate which contains the given file, nullptr is returned if not found.
             */
            ProjectDir * determineCloneCandidateRoot(Commit * c);

        }; // ProjectFile
        
        class ProjectDir {
        public:

            std::string name;
            ProjectDir * parent;

            ProjectDir(std::string const & name, ProjectDir * parent):
                name(name),
                parent(parent) {
            }

            ~ProjectDir() {
                for (auto i : subdirs_)
                    delete i.second;
                for (auto i : files_)
                    delete i.second;
            }

            std::string getName() {
                if (parent == nullptr) 
                    return "";
                std::string result = parent->getName();
                if (!result.empty())
                    result = result + "/";
                result = result + name;
                return result;
            }
            
            ProjectFile * recordChange(unsigned commit, unsigned pathId, unsigned hash) {
                // get the file
                assert(parent == nullptr);
                clones::File * f = clones::File::Get(pathId);
                ProjectDir * d = getOrCreateDir(f->parent);
                return d->updateFile(commit, pathId, hash);
            }
            
            CloneCandidate * createCloneCandidate(std::unordered_set<ProjectFile *> & changes, Commit * c);

            /** Determines if the directory, or any of its parents can be clone original for the given candidate.

                The second argument is set of already tried directories so that these can be skipped.
             */
            ProjectDir * determineCloneOriginal(CloneCandidate * cc, std::unordered_set<ProjectDir *> & visited);

            /** Returns true if every file in current dir is also present at the same hash in the other directory and if all subdirectories are transitively the same as well.
             */
            bool isSubsetOf(ProjectDir * other);

            /** Returns true if the directory only contains deleted files transitively.
             */
            bool hasOnlyDeletedFiles();

            
        private:

            friend class ProjectFile;
            
            ProjectDir * getOrCreateDir(clones::Directory * d) {
                if (d->parent == nullptr) {
                    // if the directory is root, return the root directory
                    return this;
                } else {
                    // otherwise first fix the parents
                    ProjectDir * dr = getOrCreateDir(d->parent);
                    // check if there is appropriate dir record for the directory at hand
                    auto i = dr->subdirs_.find(d->name);
                    if (i == dr->subdirs_.end())
                        i = dr->subdirs_.insert(std::make_pair(d->name, new ProjectDir(d->name, dr))).first;
                    // that's the folder we should find
                    return i->second;
                }
            }

            ProjectFile * updateFile(unsigned commit, unsigned pathId, unsigned hash) {
                clones::File * p = clones::File::Get(pathId);
                auto i = files_.find(p->filename->name);
                if (i == files_.end())
                    i = files_.insert(std::make_pair(p->filename->name, new ProjectFile(this,pathId))).first;
                i->second->update(commit, hash);
                return i->second;
            }

            ProjectDir * determineCloneCandidateRoot(clones::Commit * c, ProjectDir * result) {
                // first make sure that all files in the current directory were also added by the commit
                for (auto i : files_)
                    if (i.second->addedByCommit != c->id && i.second->hashId != 0)
                        return result;
                // now make sure that all subdirs were added by the same commit
                for (auto i : subdirs_) {
                    // if the subdir is the result so far, we know it has been checked
                    if (i.second == result)
                        continue;
                    // otherwise expect that the subdir was all added by the same commit
                    if (! i.second->expectAddedByCommit(c))
                        return result;
                }
                // now the biggest root candidate is the actual dir. Either return it if there is no parent, or try if our parent is candidate as well
                if (parent == nullptr)
                    return this;
                else
                    return parent->determineCloneCandidateRoot(c, this);
            }

            bool expectAddedByCommit(clones::Commit * c) {
                // first make sure that all files in the current directory were also added by the commit
                for (auto i : files_)
                    if (i.second->addedByCommit != c->id && i.second->hashId != 0)
                        return false;
                // now make sure that all subdirs were added by the same commit
                for (auto i : subdirs_) {
                    if (! i.second->expectAddedByCommit(c))
                        return false;
                }
                return true;
            }

            void addFile(std::string const & name, ProjectFile const * file) {
                files_.insert(std::make_pair(name, new ProjectFile(this, file)));
            }

            ProjectDir * addDirectory(std::string const & name) {
                ProjectDir * result = new ProjectDir(name, this);
                subdirs_.insert(std::make_pair(name, result));
                return result;
            } 

            void fillCloneCandidate(std::unordered_set<ProjectFile *> & changes, CloneCandidate * c, ProjectDir * dir);

            std::unordered_map<std::string, ProjectDir *> subdirs_;
            std::unordered_map<std::string, ProjectFile *> files_;
            
        };

        inline ProjectDir * ProjectFile::determineCloneCandidateRoot(clones::Commit * c) {
            return parent->determineCloneCandidateRoot(c, nullptr);
        }

        /** A candidate for a clone.

            Clone candidate is identified by the time at which it occured, the directory that is the candidate and a set of files in that directory and their actual hashes as of the time specified.

            Note that clone candidate is not interested in how many commits were used to create it, nor the states of the files before the clone candidate, etc. It is the minimal representation of the stuff that is expected to be cloned and nothing else.
         */
        class CloneCandidate {
        public:
            unsigned projectId;
            unsigned commitId;
            unsigned long time;
            unsigned originalId;
            std::string directory;
            std::unordered_map<unsigned,unsigned> files;
            /** Tree of the clone candidate.
             */
            ProjectDir tree;

            /** Converts the clone candidate to a serializable format that can be used to compare two clone candidates easily.
             */
            std::string serialize();

            /** Creates the clone candidate.
             */
            CloneCandidate():
                tree("",nullptr) {
            }


        private:
            friend std::ostream & operator << (std::ostream & s, CloneCandidate const & cc) {
                s << cc.projectId << "," << cc.commitId << "," << cc.time << "," << cc.files.size() << "," << cc.originalId << "," << helpers::escapeQuotes(cc.directory);
                return s;
            }
            
            
        }; // clones::CloneCandidate
        
        
    } // namespace dejavu::clones

    
    void DetectClones(int argc, char * argv[]);
    
} // namespace dejavu
