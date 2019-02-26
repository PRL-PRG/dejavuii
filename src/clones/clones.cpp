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
// Projects : 1180, candidates: 474566, originals: 31502, projects scanned 181975786, commits scanned 182245172

/*
  initial run:
      Projects : 20000, candidates: 67453, originals: 49301, projects scanned 203023581, commits scanned 206664871, took 8:57
  skipping projects younger than the current co:
      Projects : 20000, candidates: 67453, originals: 49301, projects scanned 156068872, commits scanned 156671561, took 8:37  
  ordering projects by time of creation:
      Projects : 20000, candidates: 67453, originals: 49301, projects scanned 150886103, commits scanned 151324605, took 10:04
  filtering projects bas[<3;74;15m]ed on file hashes as well    
      Projects : 20000, candidates: 67453, originals: 49301, projects scanned 5611924, commits scanned 5661930, took 1:48
  projects scanned ordered by creationTime
      Projects : 20000, candidates: 67453, originals: 49301, projects scanned 5999447, commits scanned 6053963, took 1:38
   It is questionable whether sorting helps

   The last thing we can do is toi limit the # of projects we see 
*/



namespace dejavu {
    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/filtered", false);
        //helpers::Option<std::string> InputDir("inputDir", "/sample621", false);
        helpers::Option<std::string> OutputDir("outputDir", "/filtered", false);


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
                // add the project to the hashes
                clones::Hash::AddProject(fileHashId, projectId);
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
                clonesOut_.open(DataRoot.value() + OutputDir.value() + "/folderClones");
                clonesOut_ << "projectId,commitId,time,numFiles,originalId,directory" << std::endl;
            }
            
            void calculate(size_t numThreads) {
                std::cout << "Ordering projects..." << std::endl;
                for (size_t i = 0; i < clones::Project::NumProjects(); ++i) {
                    clones::Project * p = clones::Project::Get(i);
                    if (p == nullptr)
                        continue;
                    projects_.insert(p);
                }
                std::cout << "Num projects: " << projects_.size();
                pIterator_ = projects_.begin();
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
                printStatus();
                std::cout << std::endl << "Writing clone originals..." << std::endl;
                clones::CloneOriginal::SaveAll(DataRoot.value() + OutputDir.value() + "/folderCloneOriginals.csv");
            }

            static std::atomic<uint64_t> NumProjectsScanned;
            static std::atomic<uint64_t> NumCommitsScanned;

        private:

            void printStatus() {
                std::lock_guard<std::mutex> g(cout_);
                std::cout << "\rProjects : " << done_ << ", candidates: " << numCc_ << ", originals: " << clones::CloneOriginal::NumOriginals() << ", projects scanned " << NumProjectsScanned << ", commits scanned " << NumCommitsScanned << "  " << std::flush;
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
                    clones::Project * p = nullptr;
                    {
                        std::lock_guard<std::mutex> g(cout_);
                        if (pIterator_ == projects_.end())
                            break;
                        p = *pIterator_;
                        ++pIterator_;
                    }
                    /*
                    size_t pid = pid_++;
                    if (pid >= clones::Project::NumProjects())
                        break;
                    clones::Project * p = clones::Project::Get(pid);
                    
                    // there may be holes in the project's array if we work on a sample
                    if (p == nullptr)
                        continue;
                    */
                    // get the clone candidates
                    cc = std::move(p->getCloneCandidates());
                    // process each clone candidate
                    for (clones::CloneCandidate * c : cc) {
                        c->originalId = clones::CloneOriginal::GetFor(c);
                        ++numCc_;
                    }
                    // output the clone candidates
                    {
                        std::lock_guard<std::mutex> g(mClonesOut_);
                        for (clones::CloneCandidate * c : cc)
                            clonesOut_ << *c << std::endl;
                    }
                    for (clones::CloneCandidate * c : cc)
                        delete c;
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
            std::ofstream clonesOut_;
            std::mutex mClonesOut_;
            std::set<clones::Project *, clones::Project::CreatedAtOrderer> projects_;
            std::set<clones::Project *, clones::Project::CreatedAtOrderer>::iterator pIterator_;
            
        }; // CloneOriginalsFinder 

        std::atomic<uint64_t> CloneOriginalsFinder::NumProjectsScanned(0);
        std::atomic<uint64_t> CloneOriginalsFinder::NumCommitsScanned(0);
        
    } // anonymous namespace


    namespace clones {
        std::vector<std::unordered_set<unsigned>> Hash::hashes_;
        std::unordered_map<std::string, Filename *> Filename::filenames_;
        std::vector<File*> File::files_;
        Directory Directory::root_("",nullptr);
        size_t Directory::numDirs_ = 0;
        std::vector<Commit*> Commit::commits_;
        std::mutex CloneOriginal::m_;
        std::unordered_map<std::string, CloneOriginal *> CloneOriginal::originals_;
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
                ++CloneOriginalsFinder::NumCommitsScanned;
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
                std::unordered_set<ProjectDir *> visited;
                ProjectDir * d = nullptr;
                for (ProjectFile * f : changes) {
                    d = f->parent->determineCloneOriginal(cc, visited);
                    if (d != nullptr)
                        break;
                }
                changes.clear();
                // if we have original candidate, then update the clone original and terminate the update for given project
                if (d != nullptr) {
                    co.projectId = id;
                    co.commitId = c->id;
                    co.time = c->time;
                    co.directory = d->getName();
                    break;
                }
                ++CloneOriginalsFinder::NumProjectsScanned;
                
            }
        }

        
        unsigned CloneOriginal::GetFor(CloneCandidate * cc) {
            std::string hash = cc->serialize();
            CloneOriginal * co;
            {
                std::lock_guard<std::mutex> g(m_);
                auto i = originals_.find(hash);
                if (i != originals_.end()) {
                    ++i->second->numOccurences;
                    return i->second->id;
                }
                // create new clone original
                unsigned id = originals_.size(); // this will be the original id *if* the original is not found
                co = new CloneOriginal(id, cc);
                originals_.insert(std::make_pair(hash, co));
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
            // decrease the set even further by checking the project file hashes in the clone
            for (auto c : cc->files) {
                std::unordered_set<unsigned> p_;
                for (auto p : projects) {
                    auto const & projects = Hash::GetFor(c.second);
                    if (projects.find(p) != projects.end())
                        p_.insert(p);
                }
                projects = p_;
            }
            // definitely the clone candidate itself is its original
            // now for each of the remaining projects, try to find an original that would be older
            std::set<Project *, Project::CreatedAtOrderer> projectsOrdered;
            for (auto pid : projects)
                projectsOrdered.insert(Project::Get(pid));
            for (Project * p: projectsOrdered) {
                //                Project * p = Project::Get(pid);
                if (p->createdAt >= co->time)
                    continue;
                p->updateCloneOriginal(cc, *co, filenames);
            }
            // the original should now be the oldest occurence of the files at the time, we are done
            return co->id;
        }

        CloneOriginal::CloneOriginal(unsigned id, CloneCandidate * cc):
            id(id),
            projectId(cc->projectId),
            commitId(cc->commitId),
            time(cc->time),
            numOccurences(1),
            directory(cc->directory) {
        }

        void CloneOriginal::SaveAll(std::string const & where) {
            std::ofstream o(where);
            o << "id,projectId,commitId,time,numOccurences,directory" << std::endl;
            for (auto i : originals_)
                o << *(i.second) << std::endl;
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
            result->commitId = c->id;
            result->time = c->time;
            result->directory = getName();
            return result;
        }

        ProjectDir * ProjectDir::determineCloneOriginal(CloneCandidate * cc, std::unordered_set<ProjectDir *> & visited) {
            if (visited.find(this) == visited.end()) {
                if (cc->tree.isSubsetOf(this))
                    return this;
                visited.insert(this);
            }
            if (parent == nullptr)
                return nullptr;
            else
                return parent->determineCloneOriginal(cc, visited);
        }

        bool ProjectDir::isSubsetOf(ProjectDir * other) {
            for (auto f : files_) {
                if (f.second->hashId == 0)
                    continue; // ignore deleted files
                auto i = other->files_.find(f.first);
                if (i == other->files_.end())
                    return false; // not found
                if (f.second->hashId != i->second->hashId) // different hash
                    return false;
            }
            // now we know files are the same, check directories
            for (auto d : subdirs_) {
                auto i = other->subdirs_.find(d.first);
                // if the directory exists in the original, it should be the same
                if (i != other->subdirs_.end()) {
                    if (! d.second->isSubsetOf(i->second))
                        return false;
                // otherwise we must be clever, the directory can be only deleted files, in which case we do not care
                } else {
                    if (! d.second->hasOnlyDeletedFiles())
                        return false;
                }
            }
            return true;
        }

        bool ProjectDir::hasOnlyDeletedFiles() {
            for (auto f: files_)
                if (f.second->hashId != 0)
                    return false;
            for (auto d : subdirs_)
                if (! d.second->hasOnlyDeletedFiles())
                    return false;
            return true;
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
