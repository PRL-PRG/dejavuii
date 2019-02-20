#include <vector>
#include <unordered_set>
#include <map>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "helpers/csv-reader.h"
#include "helpers/strings.h"

#include "../settings.h"

#include "../objects.h"

#include "clones.h"

// old version 151 mins, 140GB
// Projects : 2,405,680, candidates: 16,233,482, originals: 4,862,497
// after fix to clone candidate detecion
// Projects : 2,405,680, candidates: 12,663,129, originals: 4,909,253

namespace dejavu {
    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/filtered", false);
        //helpers::Option<std::string> InputDir("inputDir", "/sample621", false);

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



        /** Class for actually finding the originals. 
         */
        class CloneOriginalsFinder {
        public:

            CloneOriginalsFinder():
                pid_(0),
                done_(0),
                numCc_(0),
                numWorkers_(0) {
            }
            
            void calculate(size_t numThreads) {
                for (size_t i = 0; i < numThreads; ++i) {
                    std::thread t([this, i](){
                            worker(i);
                        });
                    t.detach();
                }
                {
                    std::lock_guard<std::mutex> g(cout_);
                    //std::cout << "All workers spawned..." << std::endl;
                }
                // and now wait for the worker threads to finish
                std::unique_lock<std::mutex> lm(w_);
                while (numWorkers_ != 0)
                    cv_.wait(lm);
            }

        private:

            void printStatus() {
                std::lock_guard<std::mutex> g(cout_);
                std::cout << "\rProjects : " << done_ << ", candidates: " << numCc_ << ", originals: " << clones::CloneOriginal::NumOriginals() << " " << std::flush;
            }

            void printTick() {
                std::lock_guard<std::mutex> g(cout_);
                std::cout << "* \033[1D" << std::flush;
            }

            void worker(size_t index) {
                {
                    std::lock_guard<std::mutex> g(cout_);
                    //std::cout << "Worker " << index << " started..." << std::endl;
                    std::lock_guard<std::mutex> gw(w_);
                    ++numWorkers_;
                }
                std::vector<clones::CloneCandidate *> cc;
                while (true) {
                    size_t pid = pid_++;
                    if (pid >= clones::Project::NumProjects())
                        break;
                    clones::Project * p = clones::Project::Get(pid);
                    // there may be holes in the project's array if we work on a sample
                    if (p == nullptr)
                        continue;
                    // get the clone candidates
                    cc = std::move(p->getCloneCandidates());
                    // process each clone candidate
                    for (clones::CloneCandidate * c : cc) {
                        clones::CloneOriginal::GetFor(c);
                        ++numCc_;
                        delete c;
                    }
                    cc.clear();
                    // output the debug stuff
                    if (++done_ % 20 == 0)
                        printStatus();
                    else
                        printTick();
                }
                {
                    std::lock_guard<std::mutex> g(cout_);
                    //std::cout << "Worker " << index << " terminated..." << std::endl;
                    std::lock_guard<std::mutex> gw(w_);
                    if (--numWorkers_ == 0)
                        cv_.notify_one();
                }
            }

            /** Current index of project to be read.
             */
            std::atomic<size_t> pid_;
            std::atomic<size_t> done_;
            std::atomic<size_t> numCc_;
            std::mutex cout_;
            std::mutex w_;
            std::condition_variable cv_;
            size_t numWorkers_;
            
        }; // CloneOriginalsFinder 
        
    } // anonymous namespace


    namespace clones {
        std::unordered_map<std::string, Filename *> Filename::filenames_;
        std::vector<File*> File::files_;
        Directory Directory::root_("",nullptr);
        size_t Directory::numDirs_ = 0;
        std::vector<Commit*> Commit::commits_;
        std::mutex CloneOriginal::m_;
        std::unordered_map<std::string, unsigned> CloneOriginal::originals_;
        std::vector<Project*> Project::projects_;

        std::vector<CloneCandidate * > Project::getCloneCandidates() {
            ProjectDir root("",nullptr);
            std::vector<CloneCandidate *> result;
            std::unordered_set<ProjectFile *> changes;
            //            std::cout << "Analyzing project " << id << std::endl;
            for (Commit * c : commits) {
                assert(changes.empty());
                // process the commit
                for (auto i : c->changes) {
                    unsigned pathId = i.first;
                    unsigned hash = i.second;
                    ProjectFile * x = root.recordChange(c->id, pathId, hash);
                    // only insert added files to changes
                    if (hash != 0)
                        changes.insert(x);
                }
                // if the commit does not have enough changed files 
                if (changes.size() < MIN_CLONE_FILES) {
                    changes.clear();
                    continue;
                }
                // now see if the files in the changeset constitute any clone candidates
                while (! changes.empty()) {
                    ProjectFile * f = * changes.begin(); // get the next change
                    ProjectDir * d = f->determineCloneCandidateRoot(c);
                    if (d != nullptr) {
                        CloneCandidate * cc = d->createCloneCandidate(changes, c);
                        cc->projectId = id;
                        if (cc->files.size() < MIN_CLONE_FILES)
                            delete cc;
                        else
                            result.push_back(cc);
                    } else {
                        changes.erase(f);
                    }
                }
            }
            return std::move(result);
        }

        /** This works similarly to finding the clone candidate, however we only search for original from the files that seem to be in the clone candidate and as soon as a clone original is found, we stop since there is no need to look further since any other clone original found will be younger.
         */
        void Project::updateCloneOriginal(CloneCandidate * cc, CloneOriginal & co, std::unordered_set<Filename *> filenames) {
            ProjectDir root("", nullptr);
            std::unordered_set<ProjectFile *> changes;
            for (Commit * c : commits) {
                // if the commit is younger than the original found so far, no need to look further
                if (c->time >= co.time)
                    break;
                // calculate the changes for the commit
                assert(changes.empty());
                for (auto i : c->changes) {
                    unsigned pathId = i.first;
                    // NOTE to speed things up here we can say we are only interested in filenames that appear in the candidate
                    // but if we analyze all we can also determine the type of the original relation to the clone candidate
                    unsigned hash = i.second;
                    ProjectFile * x = root.recordChange(c->id, pathId, hash);
                    // but, we only consider the file change for further analysis if it changes a filename in our filenames hash set
                    if (hash != 0 && filenames.find(File::Get(x->pathId)->filename) != filenames.end())
                        changes.insert(x);
                }
                // now changes contain all files that might actually make clone original to appear in the project, check them
                while (! changes.empty()) {
                    
                }
                // TODO expand the clone candidate for this function into a project tree so that the trees can be compared themselves in a fast manner (i.e. for each file in changes I find the possible root and then check all the paths for that root)
                
            }
        }

        
        unsigned CloneOriginal::GetFor(CloneCandidate * cc) {
            std::string hash = cc->serialize();
            unsigned id;
            {
                std::lock_guard<std::mutex> g(m_);
                id = originals_.size(); // this will be the original id *if* the original is not found 
                auto i = originals_.find(hash);
                if (i != originals_.end())
                    return i->second;
                // patch to 0
                i = originals_.insert(std::make_pair(hash, id)).first;
            }
            // get the list of projects where the clone candidate might appear, which we get by getting a set of projects containing at least the same filenames as are in the clone candidate
            auto i = cc->files.begin();
            auto e = cc->files.end();
            // set of projects that may contain the files in the candidate
            std::unordered_set<unsigned> projects = File::Get(i->first)->filename->projects;
            // set of filenames in the clone candidate
            std::unordered_set<Filename *> filenames;
            filenames.insert(File::Get(i->first)->filename);
            ++i;
            while (i != e) {
                if (projects.empty()) {
                    std::cout << "No projects for clone candidate:" << std::endl;
                    std::cout << "Project id: " << cc->projectId << ", time " << cc->time << ", directory: " << cc->directory << std::endl;
                    for (auto j : cc->files) {
                        std::cout << j.first << ", " << j.second;
                        if (*i == j)
                            std::cout << " ****";
                        std::cout << std::endl;
                    }
                }
                assert(! projects.empty() && "There should be at least one project that has all the files:)");
                Filename * o = File::Get(i->first)->filename;
                filenames.insert(o);
                std::unordered_set<unsigned> p_;
                for (auto j : projects)
                    if (o->projects.find(j) != o->projects.end())
                        p_.insert(j);
                projects = std::move(p_);
                ++i;
            }
            // definitely the clone candidate itself is its original
            CloneOriginal co(id, cc);

            // now for each of the remaining projects, try to find an original that would be older
            for (auto p : projects) 
                Project::Get(p)->updateCloneOriginal(cc, co, filenames);
            // the original should now be the oldest occurence of the files at the time, we are done
            // TODO actually be done and output the results somewhere
            return id;
        }

        CloneOriginal::CloneOriginal(unsigned id, CloneCandidate * cc):
            id(id),
            projectId(cc->projectId),
            time(cc->time),
            directory(cc->directory) {
        }
        
        std::string CloneCandidate::serialize() {
            std::map<std::string, unsigned> x;
            for (auto i : files) {
                std::string path = File::Get(i.first)->getPath().substr(directory.size());
                unsigned hash = i.second;
                x.insert(std::make_pair(path, hash));
            }
            std::stringstream s;
            for (auto i : x)
                s << i.first << ":" << i.second; 
            return s.str();
        }

        std::string File::getPath() {
            return parent->getPath() + "/" + filename->name;
        }

        std::string Directory::getPath() {
            if (parent == nullptr)
                return "";
            else
                return parent->getPath() + "/" + name;
        }

        CloneCandidate * ProjectDir::createCloneCandidate(std::unordered_set<ProjectFile *> & changes, Commit * c) {
            clones::CloneCandidate * result = new clones::CloneCandidate();
            fillCloneCandidate(changes, result, & result->tree);
            result->time = c->time;
            result->directory == getName();
            return result;
        }

        void ProjectDir::fillCloneCandidate(std::unordered_set<ProjectFile *> & changes, CloneCandidate * c, ProjectDir * dir) {
            // for each file, remove it from the changes to be investigated and record the file and its hash in the candidate
            for (auto i : files_) {
                if (i.second->hashId != 0) {
                    changes.erase(i.second);
                    c->files.insert(std::make_pair(i.second->pathId, i.second->hashId));
                    dir->addFile(i.first, i.second);
                }
            }
            // recurse in subdirectories
            for (auto i : subdirs_)
                i.second->fillCloneCandidate(changes, c, dir->addDirectory(i.first));
        }
        

    } // namespace dejavu::clones
    
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
        CloneOriginalsFinder cof;
        cof.calculate(NumThreads.value());
    }
    
} //namespace dejavu
