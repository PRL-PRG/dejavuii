#pragma once

#include <unordered_map>
#include <unordered_set>

namespace dejavu {

    namespace clones {

        constexpr unsigned MIN_CLONE_FILES = 2;

        class Project;
        class Directory;
        class File;

        /** Filename record.

            A record for each unique filename (discarding ist path). Contains the name itself and the projects it is contained in - this information is used as a triage for quickly selecting projects that might contain originals of the folders.
         */
        class Filename {
        public:
            std::string name;
            std::unordered_set<Project *> projects;

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
                projects.insert(p);
            }

        private:

            Filename(std::string const & name):
                name(name) {
            }

            static std::unordered_map<std::string, Filename *> filenames_;
            
        }; // clones::Filename

        /** File record.

            This is the leaf of the unified root structure. It contains the path id, pointer to the filename (which has the name of the file and projects containing such filename) and the parent directory. 
         */
        class File {
        public:
            Directory * parent;
            unsigned pathId;
            Filename * filename;

            static File * Get(size_t index) {
                return files_[index];
            }

            static size_t Count() {
                return files_.size();
            }

            static void Compact() {
                files_.shrink_to_fit();
            }
            
        private:
            friend class Directory;
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
            unsigned projectId;
            uint64_t time;
            std::string directory;

            /** Verifies that the original is valid for the current clone candidate.

                TODO I am not using this at all for now, will be used to double check that the hashes did not collide. 
             */
            bool verify(CloneCandidate const & cc);
        };

        /** A candidate for a clone.

            Clone candidate is identified by the time at which it occured, the directory that is the candidate and a set of files in that directory and their actual hashes as of the time specified.

            Note that clone candidate is not interested in how many commits were used to create it, nor the states of the files before the clone candidate, etc. It is the minimal representation of the stuff that is expected to be cloned and nothing else.
         */
        class CloneCandidate {
        public:
            unsigned long time;
            std::string directory;
            std::unordered_map<unsigned,unsigned> files;

            /** Determines the original of the clone candidate.

                Returns the original, or nullptr if no original was found in the dataset.
            */
            CloneOriginal * findOriginal();
            
        }; // clones::CloneCandidate

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

            static void Compact() {
                projects_.shrink_to_fit();
            }
            

            void addCommit(Commit * c) {
                commits.insert(c);
            }

            /** Creates a list of clone candidates for the project.

                We do this by creating a structure of files available in the project and go commit by commit over them. When we find a directory that has been created by a single commit, we have our clone candidate. 
             */
            std::vector<CloneCandidate *> getCloneCandidates();

            /** Detect an original for given clone candidate in the project and returns it. If none can be found, returns nullptr.
             */
            CloneOriginal * findOriginal(CloneCandidate * c);

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
        
    } // namespace dejavu::clones

    
    void DetectClones(int argc, char * argv[]);
    
} // namespace dejavu
