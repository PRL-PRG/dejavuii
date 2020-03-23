#include <algorithm>
#include <set>

#include "../loaders.h"
#include "../commands.h"

#include "../loaders.h"

#include "helpers/json.hpp"

/*

## Verify



peta@prl1e:~/devel/dejavuii/build$ time ./dejavu verify -d=/data/dejavu/join -o=/data/dejavu/verified
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects ...
Loading commits ...
Loading commit parents ...
Loading file changes ...
    failed project 275075: philpill/frostgiant
    failed project 1900875: philpill/bloodbowlnation
    TOTAL: 2 failed projects
Fixing commit times...
    333334 updates to commit times made
Verifying commit timings ...
    0 failed commits
    0 affected projects
Calculating surviving commits...
Writing projects...
Writing commits...
Writing commit parents...
Writing file changes...
Writing project structure errors...
Writing commit timings errors...
Creating symlinks...
KTHXBYE!

real    95m53.553s
user    60m47.047s
sys     53m0.616s

## Patch projects createdAt

peta@prl1e:~/devel/dejavuii/build$ time ./dejavu patch-projects-createdAt -d=/data/dejavu/verified -i=/data/dejavu/projects-metadata -ght=/data/dejavu/ghtorrent > renamedDifferent.csv
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects ...
    3542598 projects loaded

Loading file changes for project commits ...                                                                                                        Loading patch status...                                                                                                                                 0 already patched projects

Patching from ghtorrent data...                                                                                                                         110719661 projects in GHTorrent

    1843153 newly patched projects

Patching from downloaded metadata...                                                                                                                Expected gsrafael01/esgst, but rafael-gssa/esgst in project 2832395 (renamed from 750231)                                                               1566578 newly patched projects

    100201 repatched projects 

    82565 renamed

    45922 renamed existing (to be deleted)

    0 errors                                                                                                                                        KTHXBYE!                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            real    36m2.384s

user    21m48.750s

sys     11m24.359s         





peta@prl1e:~/devel/dejavuii/build$ time ./dejavu patch-projects-createdAt -d=/data/dejavu/verified -i=/data/dejavu/projects-metadata -ght=/data/dejavu/ghtorrent > renamedDifferent.csv
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects ...
    3542598 projects loaded
Loading file changes for project commits ...
Loading patch status...
    0 already patched projects
Patching from ghtorrent data...
    110719661 projects in GHTorrent
    1843153 newly patched projects
Patching from downloaded metadata...
Expected gsrafael01/esgst, but rafael-gssa/esgst in project 2832395 (renamed from 750231)
    1566578 newly patched projects
    103926 repatched projects
    82565 renamed
    45922 renamed existing (to be deleted)
    0 errors
KTHXBYE!

real    634m15.958s
user    32m25.346s
sys     601m16.972s

peta@prl1e:~/devel/dejavuii/build$ time ./dejavu filter-projects -d=/data/dejavu/verified -filter=/data/dejavu/verified/unpatchedProjects.csv -o=/data/dejavu/tmp
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects to be filtered out...
    87741 projects loaded
Filtering projects...
    3454857 valid projects
    87741 projects filtered out
Filtering file changes...
    2192450854 file changes observed
    2094606918 file changes kept
    65120069 valid commits detected
Filtering commits...
    65713337 commits observed
    65120069 commits kept
Filtering commit parents...
    70894579 links observed
    70259400 links kept
Creating symlinks...
KTHXBYE!

real    78m3.210s
user    33m30.115s
sys     44m30.579s

peta@prl1e:~/devel/dejavuii/build$ time ./dejavu detect-forks -d=/data/dejavu/patched
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects ...
3454857 projects loaded
Loading commits ...
65120069 commits loaded
Loading file changes ...
Analyzing forks...
Writing forked projects...
249746 forks found.
KTHXBYE!
real    15m16.983s
user    13m20.455s
sys     1m56.500s      

peta@prl1e:~/devel/dejavuii/build$ time ./dejavu filter-projects -d=/data/dejavu/patched -filter=/data/dejavu/patched/projectForks.csv -o=/data/dejavu/no-forks
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects to be filtered out...
    249746 projects loaded
Filtering projects...
    3205111 valid projects
    249746 projects filtered out
Filtering file changes...
    2094606918 file changes observed
    1761919755 file changes kept
    62014099 valid commits detected
Filtering commits...
    65120069 commits observed
    62014099 commits kept
Filtering commit parents...
    70259400 links observed
    66385473 links kept
Creating symlinks...
KTHXBYE!
real    71m37.097s
user    30m28.656s
sys     41m6.781s


peta@prl1e:~/devel/dejavuii/build$ time ./dejavu filter-projects -d=/data/dejavu/patched -filter=/data/dejavu/patched/projectForks.csv -o=/data/dejavu/no-forks
OH HAI CAN I HAZ DEJAVU AGAINZ?
Loading projects to be filtered out...
    249746 projects loaded
Filtering projects...
    3205111 valid projects
    249746 projects filtered out
Filtering file changes...
    2094606918 file changes observed
    1761919755 file changes kept
    62014099 valid commits detected
    Filtering commits...
    65120069 commits observed
    62014099 commits kept
Filtering commit parents...
70259400 links observed
    66385473 links kept
Creating symlinks...
KTHXBYE!
real    71m37.097s
user    30m28.656s
sys     41m6.781s

peta@prl1e:~/devel/dejavuii/build$ ./dejavu npm-filter -d=/data/dejavu/no-forks -o=data/dejavu/no-npm
OH HAI CAN I HAZ DEJAVU AGAINZ?
Filtering paths...
    303285295 total paths read
    118507844 retained paths
Loading projects ...
    3205111 total projects read
Loading commits ...
    62014099 total commits read
Loading commit parents ...
    66385473 parent records.
Loading file changes ...
    1761919755 total changes read
    481390547 changes retained
Removing empty commits...
    116840 removed commits
Writing projects...
    3186352 projects written.
Writing commits and commit parents...
    61897259 commits written
    66264068 parent records writtem
Writing file changes
    481390547 records written
Filtering paths...
    303285295 paths read
    118507844 paths retained
Creating symlinks...
KTHXBYE!


 */

namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            std::string user;
            std::string repo;
            uint64_t createdAt;
            bool fork;
            bool patched;
        };

        class Patcher {
        public:
            
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo, createdAt, false, false};
                        projects_.insert(std::make_pair(id, p));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
            }


            /** Patches the project, one by one.
             */
            void patchProjects() {
                std::cerr << "Patching from Github metadata..." << std::endl;
                for (auto i : projects_) {
                    patchProject(i.second);
                }
                std::cerr << "    " << patchedProjects_.size() << " patched projects" << std::endl;
                std::cerr << "    " << notPatched_ << " unpatched (overriden or duplicate)" << std::endl;
            }

            uint64_t iso8601ToTime(std::string const & str) {
                struct tm t;
                strptime(str.c_str(),"%Y-%m-%dT%H:%M:%SZ",&t);
                return mktime(&t);
            }

            void patchProject(Project * p) {
                std::string path = STR(p->user << "_" << p->repo);
                path = Input.value() + "/" + path.substr(0, 2) + "/" + path;
                if (helpers::FileExists(path)) {
                    nlohmann::json json;
                    std::ifstream{path} >> json;
                    // mark the project as fork, but continue so that we can run two filters allowing us do determine the two datasets size
                    if (json["fork"] == true) {
                        p->fork = true;
                    } else if (json["fork"] != false) {
                        std::cout << path << std::endl;
                        assert(false);
                    }
                    std::string jsonRepo = json["name"];
                    std::string jsonUser = json["owner"]["login"];
                    uint64_t jsonCreatedAt = iso8601ToTime(json["created_at"]);
                    // now store the project
                    std::string fullName = jsonUser + "/" + jsonRepo;
                    auto i = patchedProjects_.find(fullName);
                    // we already have a project of such name in our database so we need to figure out which one to use
                    // first we check that it is the same creationTime in the database, otherwise we would not know which project to chode
                    // then we keep the project that has identical user and repo (if any)
                    if (i != patchedProjects_.end()) {
                        assert(i->second->createdAt == jsonCreatedAt);
                        if (jsonRepo != p->repo && jsonUser != p->user) {
                            ++notPatched_;
                            return;
                        }
                    }
                    p->user = jsonUser;
                    p->repo = jsonRepo;
                    p->createdAt = jsonCreatedAt;
                    patchedProjects_[fullName] = p;
                }
            }


            void output() {
                std::ofstream f(DataDir.value() + "/patchedProjects.csv");
                std::ofstream ff(DataDir.value() + "/patchedNonForks.csv");
                std::ofstream u(DataDir.value() + "/unpatchedProjects.csv");
                std::ofstream forks(DataDir.value() + "/forks.csv");
                f << "projectId,user,repo,createdAt" << std::endl;
                size_t nonForks = 0;
                for (auto i : patchedProjects_) {
                    Project * p = i.second;
                    f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    if (p->fork == false) {
                        ++nonForks;
                        ff << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    } else {
                        forks << p->id << std::endl;
                    }
                    p->patched = true;
                }
                size_t unpatched = 0;
                for (auto i : projects_) {
                    if (i.second->patched)
                        continue;
                    ++unpatched;
                    u << i.second->id;
                }
                std::cerr << "    " << nonForks << " patched non-fork projects" << std::endl;
                std::cerr << "    " << unpatched << " unpatched projects reported" << std::endl;
            }

        private:
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<std::string, Project *> patchedProjects_;
            size_t notPatched_ = 0;
        }; 

    } // anonymous namespace

    void PatchProjectsCreatedAt(int argc, char * argv[]) {
        GhtDir.required = false;
        Input.required = false;
        Settings.addOption(DataDir);
        Settings.addOption(Input);

        Settings.parse(argc, argv);
        Settings.check();

        Patcher p;
        p.loadData();
        p.patchProjects();
        p.output();
    }
    
} // namespace dejavu

