#include <iostream>
#include <vector>
#include <unordered_map>

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


        private:

            static void AddToTriage(unsigned projectId, unsigned pathId, unsigned contentsId) {
                assert(contentsId != 0);
                uint64_t x = File::Get(pathId)->filename->id;
                x = (x << 32) | contentsId;
                ProjectsTriage_[x].insert(projectId);
            }

            static std::unordered_map<uint64_t, std::unordered_set<unsigned>> ProjectsTriage_;
        };

        std::unordered_map<std::string, Filename *> Filename::Filenames_;
        std::vector<File*> File::Files_;
        Directory * Directory::Root_ = new Directory(nullptr, "");

        std::unordered_map<unsigned, Commit *> Commit::Commits_;
        std::vector<Project *> Project::Projects_;

        std::unordered_map<uint64_t, std::unordered_set<unsigned>> FolderCloneDetector::ProjectsTriage_;

        
    } // anonymous namespace

    void DetectFolderClones(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();
        FolderCloneDetector fcd;

        
    }
    
} // namespace dejavu
