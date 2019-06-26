#include <iostream>
#include <vector>
#include <unordered_map>

#include "../loaders.h"
#include "../commands.h"


/* Jan wants:

   - # of file contents that are present in more than 1 project
 */


namespace dejavu {

    namespace {

        class Commit : public BaseCommit<Commit> {
        public:
            uint64_t time2;
            bool tainted;
            Commit(unsigned id, uint64_t time, uint64_t time2):
                BaseCommit<Commit>(id, time),
                time2(time2),
                tainted(false) {
            }
        };

        class Project : public FullProject<Project, Commit> {
        public:
            Project(unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt):
                FullProject<Project, Commit>(id, user, repo, createdAt) {
            }
        };

        

        class Filter {
        public:
            void filter() {
                {
                    std::cerr << "Filtering paths..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/paths.csv");
                    size_t totalPaths = 0;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            ++totalPaths;
                            if (!IsNPMPath(path)) {
                                f << id << "," << helpers::escapeQuotes(path) << std::endl;
                                paths_.insert(id);
                            }
                        }};
                    std::cerr << "    " << totalPaths << " total paths read" << std::endl;
                    std::cerr << "    " << paths_.size() << " retained paths" << std::endl;
                }
                // now that we have paths, we can load all projects and commits
                {
                    size_t totalProjects = 0;
                    std::cerr << "Loading projects ... " << std::endl;
                    ProjectLoader{[&,this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                            ++totalProjects;
                            if (id >= projects_.size())
                                projects_.resize(id + 1);
                            projects_[id] = new Project(id, user, repo,createdAt);
                        }};
                    std::cerr << "    " << totalProjects << " total projects read" << std::endl;
                }
                {
                    std::cerr << "Loading commits ... " << std::endl;
                    size_t totalCommits = 0;
                    CommitLoader{[&,this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                            ++totalCommits;
                            if (id >= commits_.size())
                                commits_.resize(id + 1);
                            commits_[id] = new Commit(id, authorTime, committerTime);
                        }};
                    std::cerr << "    " << totalCommits << " total commits read" << std::endl;
                }
                {
                    std::cerr << "Loading commit parents ... " << std::endl;
                    size_t count = 0;
                    CommitParentsLoader{[&count,this](unsigned id, unsigned parentId){
                            Commit * c = commits_[id];
                            Commit * p = commits_[parentId];
                            assert(c != nullptr);
                            assert(p != nullptr);
                            c->addParent(p);
                            ++count;
                        }};
                    std::cout << "    " << count << " parent records. " << std::endl;
                }
                {
                    std::cerr << "Loading file changes ... " << std::endl;
                    size_t totalChanges = 0;
                    size_t validChanges = 0;
                    FileChangeLoader{[&,this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++totalChanges;
                            if (paths_.find(pathId) != paths_.end()) {
                                ++validChanges;
                                Project * p = projects_[projectId];
                                Commit * c = commits_[commitId];
                                assert(p != nullptr);
                                assert(c != nullptr);
                                p->addCommit(c);
                                c->addChange(pathId, contentsId);
                            }
                        }};
                    std::cerr << "    " << totalChanges << " total changes read" << std::endl;
                    std::cerr << "    " << validChanges << " changes retained" << std::endl;
                }
            }

            /** Removes empty commits from the commits hierarchy.
             */
            void removeEmptyCommits() {
                std::cerr << "Removing empty commits..." << std::endl;
                size_t removed = 0;
                for (Commit * c : commits_) {
                    if (c == nullptr)
                        continue;
                    removed += detachCommitIfEmpty(c);
                }
                std::cerr << "    " << removed << " removed commits" << std::endl;
            }

            /** Now that commits have been removed, we can output the data we have.
             */
            void output() {
                helpers::EnsurePath(OutputDir.value());
                {
                    std::cerr << "Writing projects..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/projects.csv");
                    f << "projectId,user,repo,createdAt" << std::endl;
                    size_t kept = 0;
                    for (Project * p : projects_) {
                        if (p == nullptr)
                            continue;
                        bool keep = false;
                        for (Commit * c : p->commits)
                            if (! c->tainted) {
                                keep = true;
                                break;
                            }
                        if (keep) {
                            f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," <<  p->createdAt << std::endl;
                            ++kept;
                        }
                    }
                    std::cerr << "    " << kept << " projects written." << std::endl;
                }
                {
                    std::cerr << "Writing commits and commit parents..." << std::endl;
                    std::ofstream fc(OutputDir.value() + "/commits.csv");
                    std::ofstream fp(OutputDir.value() + "/commitParents.csv");
                    fc << "commitId,authorTime,committerTime" << std::endl;
                    fp << "commitId,parentId" << std::endl;
                    size_t commits = 0;
                    size_t parentRecords = 0;
                    for (Commit * c : commits_) {
                        if (c == nullptr || c->tainted)
                            continue;
                        fc << c->id << "," << c->time << "," << c->time2 << std::endl;
                        ++commits;
                        for (Commit * p : c->parents) {
                            fp << c->id << "," << p->id << std::endl;
                            ++parentRecords;
                        }
                    }
                    std::cout << "    " << commits << " commits written" << std::endl;
                    std::cout << "    " << parentRecords << " parent records writtem " << std::endl;
                }
                {
                    std::cerr << "Writing file changes" << std::endl;
                    std::ofstream f(OutputDir.value() + "/fileChanges.csv");
                    f << "projectId,commitId,pathId,contentsId" << std::endl;
                    size_t count = 0;
                    for (Project * p : projects_) {
                        if (p == nullptr)
                            continue;
                        for (Commit * c : p->commits) {
                            if ( c == nullptr || c->tainted)
                                continue;
                            for (unsigned x : c->deletions)
                                f << p->id << "," << c->id << "," << x << "," << FILE_DELETED << std::endl;
                            for (auto i : c->changes)
                                f << p->id << "," << c->id << "," << i.first << "," << i.second << std::endl;
                            count += c->deletions.size() + c->changes.size();
                        }
                    }
                    std::cerr << "    " << count << " records written" << std::endl;
                }
                {
                    std::cerr << "Filtering paths..." << std::endl;
                    std::ofstream f(OutputDir.value() + "/paths.csv");
                    f << "id,path" << std::endl;
                    size_t count = 0;
                    size_t retained = 0;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            ++count;
                            if (paths_.find(id) == paths_.end())
                                return;
                            f << id << "," << helpers::escapeQuotes(path) << std::endl;
                            ++retained;
                        }};
                    std::cerr << "    " << count << " paths read" << std::endl;
                    std::cerr << "    " << retained << " paths retained" << std::endl;
                }
            }

            
        private:


            
            size_t detachCommitIfEmpty(Commit * c) {
                if (!(c->changes.empty() && c->deletions.empty()))
                    return 0; // not empty
                if (c->tainted)
                    return 0; // already detached
                // now detach the commit itself by removing 
                for (Commit * child : c->children) {
                    child->parents.erase(c);
                    child->parents.insert(c->parents.begin(), c->parents.end());
                }
                for (Commit * parent : c->parents) {
                    parent->children.erase(c);
                    parent->children.insert(c->children.begin(), c->children.end());
                }
                c->parents.clear();
                c->children.clear();
                c->tainted = true;
                return 1;
            }



            std::unordered_set<unsigned> paths_;
            std::vector<Project*> projects_;
            std::vector<Commit*> commits_;



            
        };


        
    } //anonymous namespace

    /** Filters out any files contained in node_modules directories.
     */
    void FilterNPM(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();

        Filter f;
        f.filter();
        f.removeEmptyCommits();
        f.output();
    }
    
} // namespace dejavu



