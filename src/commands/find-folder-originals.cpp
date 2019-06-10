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

#include "../commands.h"

#include "folder_clones.h"

/** Searches for originals of clone candidates.

    
 */

namespace dejavu {

    namespace {

        class LocationHint {
        public:
            /** Adds given commit and project to the location hints.

                If there is already a location hint for the given project, it is updated if the commit's time is older than the currently stored time.
             */
            void addOccurence(Project * p, Commit * c) {
                auto i = hints_.find(p);
                if (i == hints_.end()) 
                    hints_.insert(std::make_pair(p, c->time));
                 else 
                    if (i->second > c->time)
                        i->second = c->time;
            }
        private:
            std::unordered_map<Project *, uint64_t> hints_;
            
        }; // FileLocationHint

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

            Dir * root;

            Clone(unsigned id,  SHA1Hash const & hash,unsigned occurences, unsigned files, Project * project, Commit * commit, std::string const & path):
                id(id),
                hash(hash),
                occurences(occurences),
                files(files),
                project(project),
                commit(commit),
                path(path),
                root(nullptr) {
            }

            void buildStructure(std::string const & str, std::vector<Clone *> const & clones) {
                root = new Dir(EMPTY_PATH, nullptr);
                char const * x = str.c_str();
                fillDir(root, x, clones);
                assert(*x == 0);
            }

        private:

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
                        dd->fillFrom(clones[cloneId]->root);
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
           
        };
        
        class OriginalFinder {
        public:
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
                std::cerr << "Loading path segments ... " << std::endl;
                pathSegments_.load();
                globalRoot_ = new Dir(EMPTY_PATH, nullptr);
                std::cerr << "Loading paths ... " << std::endl;
                PathToIdLoader{[&,this](unsigned id, std::string const & path){
                        if (id >= paths_.size())
                            paths_.resize(id + 1);
                        paths_[id] = globalRoot_->addPath(id, path, pathSegments_);
                    }};
                pathSegments_.clearHelpers();
                std::cerr << "Loading changes ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        p->addCommit(c);
                        c->addChange(pathId, contentsId);
                        getLocationHint(pathId, contentsId).addOccurence(p, c);
                    }};
                std::cerr << "    " << locationHints_.size() << " location hints" << std::endl;
                std::cerr << "    " << paths_.size() << " paths " << std::endl;
                std::cerr << "Loading clone candidates ..." << std::endl;
                FolderCloneLoader{DataDir.value() + "/clone_originals_candidates.csv", [this](unsigned id, SHA1Hash const & hash, unsigned occurences, unsigned files, unsigned projectId, unsigned commitId, std::string const & path){
                        if (id >= clones_.size())
                            clones_.resize(id + 1);
                        Project * p = projects_[projectId];
                        Commit * c = commits_[commitId];
                        assert(p != nullptr);
                        assert(c != nullptr);
                        clones_[id] = new Clone(id, hash, occurences, files, p, c, path);
                    }};
                std::cerr << "Loading clone structures ... " << std::endl;
                FolderCloneStructureLoader{[this](unsigned id, std::string const & str) {
                        Clone * c = clones_[id];
                        assert(c != nullptr);
                        c->buildStructure(str, clones_);
                    }};
                
            }
        private:

            LocationHint & getLocationHint(unsigned path, unsigned contents) {
                static_assert(sizeof(unsigned) * 2 == sizeof(uint64_t), "Loss of data");

                unsigned filename = paths_[path]->name;
                uint64_t id = (static_cast<uint64_t>(filename) << (sizeof(unsigned) * 8)) + contents;
                return locationHints_[id];
            }
                

                
            std::vector<Project *> projects_;
            std::vector<Commit *> commits_;
            std::vector<File *> paths_;
            PathSegments pathSegments_;
            Dir * globalRoot_;
            std::unordered_map<uint64_t, LocationHint> locationHints_;
            std::vector<Clone *> clones_;
            

            
        }; // OriginalFinder
        
    } // anonymous namespace


    void FindFolderOriginals(int argc, char * argv[]) {
        NumThreads.updateDefaultValue(8);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();

        OriginalFinder f;
        f.loadData();
        
    }
    
} // namespace dejavu
