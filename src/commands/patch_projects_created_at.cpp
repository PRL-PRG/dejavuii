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
            bool patched;

            std::set<unsigned> commits;

            unsigned commonCommitsWith(Project * other) {
                std::vector<unsigned> x;
                std::set_intersection(commits.begin(), commits.end(), other->commits.begin(), other->commits.end(), std::back_inserter(x));
                return x.size();
            }
        };

        class Patcher {
        public:
            
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        Project * p = new Project{id, user, repo, createdAt, false};
                        projects_.insert(std::make_pair(id, p));
                        projectsByName_.insert(std::make_pair(user + "/" + repo, p));
                    }};
                std::cerr << "    " << projects_.size() << " projects loaded" << std::endl;
                std::cerr << "Loading file changes for project commits ... " << std::endl;
                FileChangeLoader{[this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                        Project * p = projects_[projectId];
                        assert(p != nullptr);
                        p->commits.insert(commitId);
                    }};
                {
                    size_t patched = 0;
                    std::cerr << "Loading patch status..." << std::endl;
                    std::string filename = DataDir.value() + "/patchedProjects.csv";
                    if (helpers::FileExists(filename)) {
                        IdLoader{filename, [&, this](unsigned projectId) {
                                auto i = projects_.find(projectId);
                                assert(i != projects_.end());
                                assert(i->second->patched == false);
                                i->second->patched = true;
                                ++patched;
                            }};
                    }
                    std::cerr << "    " << patched << " already patched projects" << std::endl;
                }
            }

            void patchFromGhTorrent() {
                if (!GhtDir.isSpecified())
                    return;
                std::cerr << "Patching from ghtorrent data..." << std::endl;
                size_t total = 0;
                size_t patched = 0;
                GHTorrentProjectsLoader(GhtDir.value() + "/projects.csv", [&,this](unsigned id,
                                                 std::string const & url,
                                                 unsigned ownerId,
                                                 std::string const & name,
                                                 std::string const & description,
                                                 std::string const & language,
                                                 uint64_t createdAt,
                                                 unsigned forkedFrom,
                                                 uint64_t deleted,
                                                 uint64_t updatedAt){
                        // first get the user and repo strings from the url and check if we have the project in our database
                        ++total;
                        std::string index;
                        if (url.find("https://api.github.com/repos/") == 0) {
                            index = url.substr(29);
                        } else if (url.find("https://api./repos/") == 0) {
                            index = url.substr(19);
                        } else if (url == "\\N") {
                            return; // skip projects w/o urls
                        } else {
                            std::cerr << "Invalid url format: " << url << std::endl;
                            return;
                        }
                        auto i = projectsByName_.find(index);
                        if (i == projectsByName_.end())
                            return;
                        Project * p = i->second;
                        // now update createdAt
                        ++patched;
                        if (p->patched) {
                            if (p->createdAt < createdAt)
                                p->createdAt = createdAt;
                        } else {
                            p->createdAt = createdAt;
                            p->patched = true;
                        }
                    });
                std::cerr << "    " << total << " projects in GHTorrent" << std::endl;
                std::cerr << "    " << patched << " newly patched projects" << std::endl;
                
            }

            uint64_t iso8601ToTime(std::string const & str) {
                struct tm t;
                strptime(str.c_str(),"%Y-%m-%dT%H:%M:%SZ",&t);
                return mktime(&t);
            }

            static constexpr int NOT_FOUND = -1;
            static constexpr int PATCHED = 0;
            static constexpr int REPATCHED = 1;
            static constexpr int RENAMED_NEW = 2;
            static constexpr int RENAMED_EXISTING = 3;
            static constexpr int RENAMED_EXISTING_NOT_DOWNLOADED = 4;

            int patchProjectFromMetadata(Project * p) {
                int result = PATCHED;
                std::string path = STR(Input.value() << "/" << (p->id % 1000) << "/" << p->id);
                nlohmann::json json;
                std::string fullName;
                // if the metadata file exists, parse the json
                if (helpers::FileExists(path)) {
                    std::ifstream (path) >> json;
                    fullName = helpers::ToLower(json["full_name"]);
                } else if (helpers::FileExists(path + ".renamed")) {
                    std::ifstream (path + ".renamed") >> json;
                    fullName = helpers::ToLower(json["full_name"]);
                    auto i = projectsByName_.find(fullName);
                    // the project does not exist yet so all we have to do is to rename the existing project
                    if (i == projectsByName_.end()) {
                        std::vector<std::string> split = helpers::Split(fullName, '/', 2);
                        p->user = split[0];
                        p->repo = split[1];
                        result = RENAMED_NEW;
                        // remove the renamed suffix from the metadata
                        system(STR("mv " << path << ".renamed " << path).c_str());
                    // if the project exists,
                    } else {
                        Project * np = i->second;
                        if (fullName != np->user + "/" + np->repo) {
                            std::cerr << "Expected " << fullName << ", but " << np->user << "/" << np->repo << " in project " << np->id << " (renamed from "<< p->id << ")" << std::endl;
                            return NOT_FOUND;
                        }
                        //assert(fullName == np->user + "/" + np->repo);
                        // check that the old project is clone of the new one
                        if (p->commonCommitsWith(np) != 0)
                            std::cout << p->id << ","<< helpers::escapeQuotes(p->user + "/" + p->repo) << "," << helpers::escapeQuotes(fullName) << std::endl;
                                                                                                                                                //assert(p->commonCommitsWith(np) != 0);
                        // check if the metadata for the new project exists, if it does, there is nothing to do, but if it does
                        // not, copy the existing and then patch the project
                        std::string npath = STR(Input.value() << "/" << (np->id % 1000) << "/" << np->id);
                        if (helpers::FileExists(npath)) {
                            system(STR("mv " << path << ".renamed " << path << ".deleted").c_str());
                            return RENAMED_EXISTING;
                        } else {
                            system(STR("mv " << path << ".renamed " << npath).c_str());
                            patchProjectFromMetadata(np);
                            return RENAMED_EXISTING_NOT_DOWNLOADED;
                        }
                    }
                } else {
                    return NOT_FOUND;
                }
                // check that the metadata correspond to the project
                assert(fullName == p->user + "/" + p->repo);
                p->createdAt = iso8601ToTime(json["created_at"]);
                if (p->patched) 
                    result = REPATCHED;
                p->patched = true;
                return result;
            }
            

            /** The github metadata is a bit more complicated.

                First rule is, if we have the metadata valid for the project, we use it and it overides anything else we might have in our database.

             */
            void patchFromGithubMetadata() {
                if (!Input.isSpecified())
                    return;
                std::cerr << "Patching from downloaded metadata..." << std::endl;
                size_t errors = 0;
                size_t patched = 0;
                size_t repatched = 0;
                size_t renamedNew = 0;
                size_t renamed = 0;
                size_t total = 0;
                std::ofstream tbd(DataDir.value() + "/oldProjects.csv");
                tbd << "projectId,user,repo" << std::endl;
                for (auto i : projects_) {
                    Project * p = i.second;
                    if (total++ % 1000 == 0)
                        std::cerr << "    " << (total/1000) << "k    \r" << std::flush;
                    try {
                        switch (patchProjectFromMetadata(p)) {
                        case NOT_FOUND:
                            break;
                        case PATCHED:
                            ++patched;
                            break;
                        case REPATCHED:
                            ++repatched;
                            break;
                        case RENAMED_NEW:
                            ++renamedNew;
                            break;
                        case RENAMED_EXISTING:
                        case RENAMED_EXISTING_NOT_DOWNLOADED:
                            ++renamed;
                            // output the project to be deleted
                            tbd << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << std::endl;
                            break;
                        }
                    } catch (...) {
                        ++errors;
                    }
                }
                std::cerr << "    " << patched << " newly patched projects" << std::endl;
                std::cerr << "    " << repatched << " repatched projects " << std::endl;
                std::cerr << "    " << renamedNew << " renamed" << std::endl;
                std::cerr << "    " << renamed << " renamed existing (to be deleted)" << std::endl;
                std::cerr << "    " << errors << " errors" << std::endl;
            }

            void output() {
                std::ofstream f(DataDir.value() + "/projects.csv");
                std::ofstream pp(DataDir.value() + "/patchedProjects.csv");
                std::ofstream up(DataDir.value() + "/unpatchedProjects.csv");
                f << "projectId,user,repo,createdAt" << std::endl;
                pp << "projectId" << std::endl;
                up << "projectId,user,repo,createdAt" << std::endl;
                size_t patched = 0;
                for (auto i : projects_) {
                    Project * p = i.second;
                    f << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    if (p->patched) {
                        ++patched;
                        pp << p->id << std::endl;
                    } else {
                        up << p->id << "," << helpers::escapeQuotes(p->user) << "," << helpers::escapeQuotes(p->repo) << "," << p->createdAt << std::endl;
                    }
                }
                std::cout << "    " << patched << " patched projects after the stage" << std::endl;


                
            }

        private:
            std::unordered_map<unsigned, Project *> projects_;
            std::unordered_map<std::string, Project *> projectsByName_;


            
        }; 

    } // anonymous namespace


    void PatchProjectsCreatedAt(int argc, char * argv[]) {
        GhtDir.required = false;
        Input.required = false;
        Settings.addOption(DataDir);
        Settings.addOption(Input);
        Settings.addOption(GhtDir);
        Settings.parse(argc, argv);
        Settings.check();

        Patcher p;
        p.loadData();
        p.patchFromGhTorrent();
        p.patchFromGithubMetadata();
        p.output();
        
    }
    
} // namespace dejavu

