#include <vector>
#include <unordered_set>
#include "helpers/csv-reader.h"
#include "helpers/strings.h"

#include "../settings.h"

#include "../objects.h"

#include "clones.h"

// old version 151 mins, 140GB 

namespace dejavu {
    namespace {

        helpers::Option<std::string> InputDir("inputDir", "/sample", false);


        class ProjectsReader : public Project::Reader {
        protected:
            void onRow(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt, int fork, unsigned committers, unsigned authors, unsigned watchers) override {
                clones::Project::Create(id, createdAt);
            }

            void onDone(size_t numRows) override {
                std::cout << "Loaded " << numRows << " projects" << std::endl;
            }
            
            
        }; 
        
        class CommitsReader : public Commit::Reader {
        protected:
            void onRow(unsigned id, std::string const & hash, uint64_t time, unsigned numProjects, unsigned originalProject) {
                clones::Commit::Create(id, time, originalProject);
            }

            void onDone(size_t numRows) override {
                std::cout << "loaded " << numRows << " commits" << std::endl;
            }
        };

        class PathsReader : public Path::Reader {
        protected:
            void onRow(unsigned id, std::string const & path) override {
                clones::Directory::Root()->addPath(id, path);
            }

            void onDone(size_t numRows) override {
                std::cout << "Loaded " << numRows << " unique paths." << std::endl;
                std::cout << "    unique filenames " << clones::Filename::Count() << std::endl;
                std::cout << "    files created " << clones::File::Count() << std::endl;
                std::cout << "    directories " << clones::Directory::Count() << std::endl;
            }
        };

        class ChangesReader : public FileChange::Reader {
        protected:
            void onRow(unsigned projectId, unsigned pathId, unsigned fileHashId, unsigned commitId) override {
                // add the change to the commit
                clones::Commit * c = clones::Commit::Get(commitId);
                c->addChange(pathId, fileHashId);
                // add the commit to the project
                clones::Project * p = clones::Project::Get(projectId);
                p->addCommit(c);
                // add the project to the filename
                clones::File * f = clones::File::Get(pathId);
                f->filename->addProject(p);
                
            }
                
            void onDone(size_t numRows) override {
                std::cout << "Analyzed " << numRows << " file changes" << std::endl;
            }
            
        };

        class DirRecord;
        
        class FileRecord {
        public:
            DirRecord * parent;
            unsigned pathId;
            unsigned hashId;
            unsigned addedByCommit;

            FileRecord(DirRecord * parent, unsigned pathId):
                parent(parent),
                pathId(pathId),
                hashId(0),
                addedByCommit(0) {
            }

            void update(unsigned commit, unsigned hash) {
                if (hashId == 0) {
                    assert(hash != 0);
                    addedByCommit = commit;
                }
                hashId = hash;
            }
        };

        
        class DirRecord {
        public:

            std::string name;
            DirRecord * parent;

            DirRecord(std::string const & name, DirRecord * parent):
                name(name),
                parent(parent) {
            }

            ~DirRecord() {
                for (auto i : subdirs_)
                    delete i.second;
                for (auto i : files_)
                    delete i.second;
            }
            
            FileRecord * recordChange(unsigned commit, unsigned pathId, unsigned hash) {
                // get the file
                assert(parent == nullptr);
                clones::File * f = clones::File::Get(pathId);
                DirRecord * d = getOrCreateDir(f->parent, this);
                return d->updateFile(commit, pathId, hash);
            }

            DirRecord * determineCloneCandidate(clones::Commit * c) {
                // if the dir is not leaf, then either it can't even be a candidate, or there will be another file in the commit that will be leaf
                if (! subdirs_.empty())
                    return nullptr;
                // determine the cloneCandidate
                return determineCloneCandidate(c, nullptr);
            }

            clones::CloneCandidate * createCloneCandidate(std::unordered_set<FileRecord *> & changes, clones::Commit * c) {
                clones::CloneCandidate * result = new clones::CloneCandidate();
                fillCloneCandidate(changes, result);
                result->time = c->time;
                result->directory == getName();
                return result;
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
            
        private:

            void fillCloneCandidate(std::unordered_set<FileRecord *> & changes, clones::CloneCandidate * c) {
                // for each file, remove it from the changes to be investigated and record the file and its hash in the candidate
                for (auto i : files_) {
                    changes.erase(changes.find(i.second));
                    c->files.insert(std::make_pair(i.second->pathId, i.second->hashId));
                }
                // recurse in subdirectories
                for (auto i : subdirs_)
                    i.second->fillCloneCandidate(changes, c);
            }

            bool verifyCloneCandidate(clones::Commit * c) {
                for (auto i  : subdirs_)
                    if (! i.second->verifyCloneCandidate(c))
                        return false;
                for (auto i : files_)
                    if (i.second->addedByCommit != c->id)
                        return false;
                return true;
            }

            DirRecord * determineCloneCandidate(clones::Commit * c, DirRecord * child) {
                for (auto i : subdirs_)
                    if (! i.second->verifyCloneCandidate(c))
                        return child;
                for (auto i : files_)
                    if (i.second->addedByCommit != c->id)
                        return child;
                if (parent == nullptr)
                    return this;
                else
                    return parent->determineCloneCandidate(c, this);
            }

            DirRecord * getOrCreateDir(clones::Directory * d, DirRecord * root) {
                if (d->parent == nullptr) {
                    // if the directory is root, return the root directory
                    return root;
                } else {
                    // otherwise first fix the parents
                    DirRecord * dr = getOrCreateDir(d->parent, root);
                    // check if there is appropriate dir record for the directory at hand
                    auto i = subdirs_.find(d->name);
                    if (i == subdirs_.end())
                        i = subdirs_.insert(std::make_pair(d->name, new DirRecord(d->name, dr))).first;
                    // that's the folder we should find
                    return i->second;
                }
            }

            FileRecord * updateFile(unsigned commit, unsigned pathId, unsigned hash) {
                clones::File * p = clones::File::Get(pathId);
                auto i = files_.find(p->filename->name);
                if (i == files_.end())
                    i = files_.insert(std::make_pair(p->filename->name, new FileRecord(this,pathId))).first;
                i->second->update(commit, hash);
                return i->second;
            }

            std::unordered_map<std::string, DirRecord *> subdirs_;
            std::unordered_map<std::string, FileRecord *> files_;
            
        };
        
    } // anonymous namespace

    namespace clones {
        
        std::unordered_map<std::string, Filename *> Filename::filenames_;
        std::vector<File*> File::files_;
        Directory Directory::root_("",nullptr);
        size_t Directory::numDirs_ = 0;
        std::vector<Commit*> Commit::commits_;
        std::vector<Project*> Project::projects_;



        /**
         */
        CloneOriginal * CloneCandidate::findOriginal() {
            // TODO determine if we already calculated the original and if so, return the cached original
            // determine the list of projects to investigate by narrowing the number of projects to search
            auto i = files.begin();
            std::unordered_set<Project *> projects = File::Get(i->first)->filename->projects;
            ++i;
            while (i != files.end()) {
                assert(! projects.empty() && "There should be at least one project that has all the files:)");
                Filename * o = File::Get(i->first)->filename;
                std::unordered_set<Project *> p_;
                for (auto i : projects)
                    if (o->projects.find(i) != o->projects.end())
                        p_.insert(i);
                projects = std::move(p_);
            }
            CloneOriginal * result = nullptr;
            // now analyze each project
            for (Project * p : projects) {
                CloneOriginal * co = p->findOriginal(this);
            }
            
        }

        std::vector<CloneCandidate * > Project::getCloneCandidates() {
            DirRecord root("",nullptr);
            std::vector<CloneCandidate *> result;
            std::unordered_set<FileRecord *> changes; 
            for (Commit * c : commits) {
                changes.clear();
                // process the commit
                for (auto i : c->changes) {
                    unsigned pathId = i.first;
                    unsigned hash = i.second;
                    changes.insert(root.recordChange(c->id, pathId, hash));
                }
                // now see if any of the files is a starting point for a clone candidate
                if (changes.size() > MIN_CLONE_FILES) {
                    while (!changes.empty()) {
                        FileRecord * f = * changes.begin();
                        changes.erase(changes.begin());
                        // start from the change and determine the largest subfolder created by the commit
                        DirRecord * d = f->parent->determineCloneCandidate(c);
                        if (d != nullptr) 
                            result.push_back(d->createCloneCandidate(changes, c));
                    }
                }
            }
            std::cout << "Project " << id << ", found " << result.size() << " clone candidates:" << std::endl;
            for (CloneCandidate * c: result)
                std::cout << "    " << c->directory << ", at: " << c->time << ", files: " << c->files.size() << std::endl;
            return std::move(result);
        }

        /** First determine which directories can be the originals.
         */
        CloneOriginal * Project::findOriginal(CloneCandidate * c) {
            
            return nullptr;
        }

    } // namespace clones

    void DetectClones(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.parse(argc, argv);
        settings.check();
        {
            ProjectsReader pr;
            pr.readFile(DataRoot.value() + "/" + InputDir.value() + "/projects.csv", true);
            clones::Project::Compact();
        }
        {
            CommitsReader cr;
            cr.readFile(DataRoot.value() + "/" + InputDir.value() + "/commits.csv", true);
            clones::Commit::Compact();
        }
        {
            PathsReader pr;
            pr.readFile(DataRoot.value() + "/" + InputDir.value() + "/paths.csv");
            clones::File::Compact();
        }
        {
            ChangesReader cr;
            cr.readFile(DataRoot.value() + "/" + InputDir.value() + "/files.csv");
        }
        // now that we have loaded everything, it is time to look at the clone candidates, per project
        // TODO this parallelizes trivially
        
        for (clones::Project * p : clones::Project::All()) {
            std::vector<clones::CloneCandidate *> cc = p->getCloneCandidates();
            for (auto i : cc)
                delete i;
        }
        //        FolderCloneDetector::Detect();
        //        while (true) {} 
    }
    
} //namespace dejavu
