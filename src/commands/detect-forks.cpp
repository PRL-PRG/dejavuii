#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {


    class Project;
    
    class Commit : public BaseCommit<Commit> {
    public:

            
        Commit(unsigned id, uint64_t time):
            BaseCommit<Commit>(id, time) {
        }

        void determineForks();
        
        std::unordered_set<Project*> projects;

    };
    

    class Project : public BaseProject<Project, Commit> {
    public:
        Project * forkOf;
        Project(unsigned id, uint64_t createdAt):
            BaseProject<Project, Commit>(id, createdAt),
            forkOf(nullptr) {
        }

        void addCommit(Commit * c) {
            commits.insert(c);
            c->projects.insert(this);
        }
    };

    
    /** Detects projects that are forks.

        

     */
    class ForkDetector {
    public:
        void loadData() {
            std::cerr << "Loading projects ... " << std::endl;
            ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                    assert(projects_.find(id) == projects_.end());
                    projects_.insert(std::make_pair(id, new Project(id, createdAt)));
                }};
            std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
            std::cerr << "Loading commits ... " << std::endl;
            CommitLoader{[this](unsigned id, uint64_t authorTime, uint64_t committerTime){
                    assert(commits_.find(id) == commits_.end());
                    commits_.insert(std::make_pair(id, new Commit(id, authorTime)));
                }};
            std::cerr << "    " << commits_.size() << " commits loaded" << std::endl;
            std::cerr << "Loading file changes ... " << std::endl;
            FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                    Project * p = projects_[projectId];
                    Commit * c = commits_[commitId];
                    assert(p != nullptr);
                    assert(c != nullptr);
                    p->addCommit(c);
                }};
        }

        void findForks() {
            std::cerr << "Analyzing forks..." << std::endl;
            for (auto c : commits_)
                c.second->determineForks();
        }

        void write() {
            std::cerr << "Writing forked projects..." << std::endl;
            std::ofstream f(DataDir.value() + "/projectForks.csv");
            f << "project,forkOf" << std::endl;
            size_t forks = 0;
            for (auto p : projects_)
                if (p.second->forkOf != nullptr) {
                    ++forks;
                    f << p.second->id << "," << p.second->forkOf->id << std::endl;
                }
            std::cerr << "    " << forks << " forks found." << std::endl;
        }

    private:
        std::unordered_map<unsigned, Project*> projects_;
        std::unordered_map<unsigned, Commit*> commits_;
        
    };

    void Commit::determineForks() {
        if (projects.size() == 1)
            return;
        // find the oldest project
        Project * original = * projects.begin();
        for (Project * p : projects)
            if (p->createdAt < original->createdAt)
                original = p;
        // mark all non-originals as forks
        for (Project * p : projects)
            if (p != original)
                p->forkOf = original;
    }
    


    void DetectForks(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.parse(argc, argv);
        Settings.check();

        ForkDetector fd;
        fd.loadData();
        fd.findForks();
        fd.write();
    }
    
} // namespace dejavu
