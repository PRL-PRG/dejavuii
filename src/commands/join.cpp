#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <string>

#include "helpers/strings.h"

#include "../commands.h"
#include "../loaders.h"
#include "../commit_iterator.h"



namespace dejavu {

    namespace {

        /** Information about a single commit taken.
         */
        class Commit {
        public:
            unsigned id;
            
            /** Hash of the commit. */
            std::string hash;

            /** Ids of the parent commits. */
            std::unordered_set<Commit *> parents;
            std::unordered_set<Commit *> children;

            /** Changes made to files by the commit (pathId -> contentsId)
             */
            std::unordered_map<unsigned, unsigned> changes;

            /** Commit message.
             */
            std::string message;

            std::string authorEmail;
            uint64_t authorTime;
            std::string committerEmail;
            uint64_t committerTime;
            std::unordered_set<std::string> tags;

            Commit(std::string const & hash, std::string const & authorEmail, uint64_t authorTime, std::string const & committerEmail, uint64_t committerTime, std::string const & tag);


            // Commit iterator interface
            unsigned numParentCommits() const {
                return parents.size();
            }

            std::unordered_set<Commit *> const & childrenCommits() const {
                return children;
            }

            /** Adds the specified change to the commit.

                It may happen that a change to the path specified has already been recorded in the following cases:

                - the first change is a delete, the current change is update. This can happen if a commit renames two files A -> B and C -> A. We might first see the delete of A and create of B an then delete of C and create of A
                - the first change is valid, the second change is delete (the reverse of the above - there is no order for changes in single commit)
                - multiple updates to the same file as long as all updates change the path to the same contents id (merge commits report diffs to all their parents)
             */
            void addChange(unsigned pathId, unsigned contentsId) {
                auto i = changes.find(pathId);
                if (i == changes.end()) {
                    changes.insert(std::make_pair(pathId, contentsId));
                } else {
                    if (i->second == FILE_DELETED) {
                        i->second = contentsId;
                    } else {
                        if (contentsId != FILE_DELETED)
                            assert(contentsId == i->second);
                    }
                }
            }

            /** Detaches the commit from the hierarchy of commits.
             */
            void detachFromHierarchy() {
                // from all chidren, erase the parent link to the commit and add parent links to all parents of the current commit
                for (Commit * c : children) {
                    c->parents.erase(this);
                    for (Commit * p : parents)
                        c->parents.insert(p);
                }
                // from all parents, erase the child link and add all children of the current commit, also add any tags of current commit to the tags of parent
                for (Commit * p : parents) {
                    p->children.erase(this);
                    for (Commit * c : children)
                        p->children.insert(c);
                    for (std::string const & t : tags)
                        p->tags.insert(t);
                }
            }

            /** Removes unnecessary edges to children.

                Child edge is unnecessary if all other children of the commit eventually have the child as their children as well. 
             */
            unsigned compactChildEdges() {
                if (children.size() < 2)
                    return 0;
                std::vector<Commit *> toBeRemoved;
                for (Commit * c : children) {
                    if (c->parents.size() < 2)
                        continue;
                    std::unordered_set<Commit *> visited;
                    if (isDominatedBy(c, visited))
                        toBeRemoved.push_back(c);
                }
                for (Commit * r : toBeRemoved) {
                    children.erase(r);
                    r->parents.erase(this);
                }
                return toBeRemoved.size();
            }

            bool isMasterHead(std::string const & expectedTag) {
                if (tags.empty()) return 0;
                for (std::string const & tag : tags) {
                    if (tag.find(expectedTag) != std::string::npos)
                        return true;
                }
                return false;
            }

        private:

            bool isDominatedBy(Commit * child, std::unordered_set<Commit *> & visited) {
                if (this == child)
                    return true;
                if (visited.find(this) != visited.end())
                    return true;
                for (Commit * c : children)
                    if (! c->isDominatedBy(child, visited))
                        return false;
                visited.insert(this);
                return true;
            }
        };


        
        class SubmoduleInfo {
        public:

            SubmoduleInfo() = default;
            
            SubmoduleInfo(SubmoduleInfo const & other):
                submodules(other.submodules) {
            }

            void mergeWith(SubmoduleInfo const & other, Commit *) {
                for (auto s : other.submodules) {
                    submodules.insert(s);
                    if (submodulesPendingDelete.find(s) != submodulesPendingDelete.end())
                        submodulesPendingDelete.erase(s);
                }
                for (auto s : other.submodulesPendingDelete) {
                    if (submodules.find(s) == submodules.end())
                        submodulesPendingDelete.insert(s);
                }
            }

            /** Determine if the commit changes the .gitmodules path and if it does, update the submodules information according to the contents of that file.
             */
            void updateWith(Commit * c, std::string const & path);

            /** Path ids of submodule files.
             */
            std::unordered_set<unsigned> submodules;
            /** It may happen that a submodule is deleted from gitmodules, but the actual file stays in the repo. In this case the path is moved to this set and any delete of a path from this set is ignored.

                Both change and delete of a path in this list removes the path from the list.
             */
            std::unordered_set<unsigned> submodulesPendingDelete;
        };


        class Project {
        public:
            static std::string MangleName(std::string const & user, std::string const & repo) {
                return user + "_" + repo;
            }

            unsigned id;
            std::string user;
            std::string repo;
            uint64_t createdAt; // the oldest commit
            bool containsSubmodules_;

            std::unordered_map<std::string, Commit *> commits;

            // TODO this should be converted to project objects
            
            typedef std::unordered_set<Commit*> COMMIT_SET;
            COMMIT_SET commits_;
            
            COMMIT_SET::iterator commitsBegin() {
                return commits_.begin();
            }

            COMMIT_SET::iterator commitsEnd() {
                return commits_.end();
            }

            bool hasCommit(Commit * c) const {
                return commits.find(c->hash) != commits.end();
            }
            
            Project(unsigned id, std::string const & user, std::string const & repo):
                id(id),
                user(user),
                repo(repo),
                createdAt(-1),
                containsSubmodules_(false) {
            }

            ~Project() {
                for (auto i : commits) {
                    delete i.second;
                }
            }

            std::string getPath(std::string const & where) {
                std::string mn = MangleName(user, repo);
                if (user.size() >= 3) 
                    return STR(where << "/" << mn.substr(0,3) << "/" << mn);
                else
                    return STR(where << "/" << user << "/" << mn);
            }

            /** Loads commits metadata for the given project.
             */
            void loadCommits(std::string const & path);

            Commit * getMasterHead(std::string const & expectedTag);

            /** Keeps only commits on the master branch.
              */
            void filterMasterBranch();

            /** Removes changes to submodules from the projects so that they are not treated as files.
              */
            void ignoreSubmodules(std::string const & path);

            /** Removes commits that do not have any valid file changes from the set of commits the project has.
             */
            void removeEmptyCommits();

            void compactCommitHierarchy();

            void write();
        };

        /** Analyzes and compacts a single project.

            Refactored in its own class in case we ever go multithreaded on the joiner.  
         */
        class ProjectAnalyzer {
        public:

            static bool IsValidPath(std::string const & path) {
                return helpers::endsWith(path, ".js") || helpers::endsWith(path, ".json") || path == ".gitmodules";
            }

            static void Initialize() {
                // make sure the output directory exists, create if not
                helpers::EnsurePath(DataDir.value());
                // see if there are hashes from previous files, i.e. already completed, if they are, load them,
                // if not, make sure that the hash id for hash of all zeros used by github to do deleted files is hash id 0
                std::cerr << "Loading translated hashes..." << std::endl;
                std::string hashes = DataDir.value() + "/hashes.csv";
                if (helpers::FileExists(hashes)) {
                    HashToIdLoader{hashes, hashToId_};
                    hashes_.open(hashes, std::ios_base::app);
                } else {
                    hashes_.open(hashes);
                    hashes_ << "#id, hash" << std::endl;
                    GetOrCreateHashId("0000000000000000000000000000000000000000");
                }
                assert(GetOrCreateHashId("0000000000000000000000000000000000000000") == FILE_DELETED);
                // load the previously seen paths
                std::cerr << "Loading translated paths..." << std::endl;
                std::string paths = DataDir.value() + "/paths.csv";
                if (helpers::FileExists(paths)) {
                    PathToIdLoader{paths, pathToId_};
                    paths_.open(paths, std::ios_base::app);
                } else {
                    paths_.open(paths);
                    paths_ << "#id, path" << std::endl;
                }
                // load previously seen users
                std::cerr << "Loading translated users..." << std::endl;
                std::string users = DataDir.value() + "/users.csv";
                if (helpers::FileExists(users)) {
                    UsersLoader{users, [](unsigned id, std::string const & email){
                            userToId_.insert(std::make_pair(email, id)); 
                        }};
                    users_.open(users, std::ios_base::app);
                } else {
                    users_.open(users);
                    users_ << "#id, email" << std::endl;
                }
                // see if there are any previously completed projects and load the set of these as well
                std::cerr << "Loading already completed projects..." << std::endl;
                std::string projects = DataDir.value() + "/projects.csv";
                if (helpers::FileExists(projects)) {
                    ProjectLoader{projects, [](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                            completedProjects_.insert(Project::MangleName(user, repo));
                        }};
                } else {
                    std::ofstream p(projects);
                    p << "#id, user, repo, createdAt" << std::endl;
                }
                // prepare the fileChanges, commits, commitAuthors and commitMessages file headers if these do not exist
                std::string filename = DataDir.value() + "/fileChanges.csv";
                if (!helpers::FileExists(filename)) {
                    std::ofstream f(filename);
                    f << "#project id, commit id, path id, contents id" << std::endl;
                }
                filename = DataDir.value() + "/commits.csv";
                if (!helpers::FileExists(filename)) {
                    std::ofstream f(filename);
                    f << "#commit id, author time, committer time" << std::endl;
                }
                filename = DataDir.value() + "/commitAuthors.csv";
                if (!helpers::FileExists(filename)) {
                    std::ofstream f(filename);
                    f << "#commit id, author id, committer id" << std::endl;
                }
                /*
                filename = DataDir.value() + "/commitMessages.csv";
                if (!helpers::FileExists(filename)) {
                    std::ofstream f(filename);
                    f << "#commit id, message" << std::endl;
                }
                */
                filename = DataDir.value() +"/commitParents.csv";
                if (!helpers::FileExists(filename)) {
                    std::ofstream f(filename);
                    f << "#commit id, parent id" << std::endl;
                }
                
            }

            //            tar -zxf repos-10.tar.gz -C /home/peta/xxxxxx
            static void AnalyzeDir() {
                std::cerr << "Analyzing directory " << DownloaderDir.value() << std::endl;
                helpers::DirectoryReader(DownloaderDir.value(), [](std::string const & filename) {
                        if (! helpers::endsWith(filename,".tar.gz")) {
                            std::cerr << "Skipping file " << filename << std::endl;
                            return;
                        }
                        std::cerr << "Analyzing file " << filename << std::endl;
                        std::cout << "Chunk " << filename << std::endl;
                        helpers::TempDir t(TempDir.value());
                        std::cerr << "Decompressing " << filename << " into " << t.path() << "..." << std::endl;
                        helpers::System(STR("tar -zxf " << filename << " -C " << t.path()));
                        std::cerr << "Analyzing the chunk..." << std::endl;
                        Analyze(t.path());
                    });
            }


            static void Analyze(std::string const & path) {
                std::string timings = path + "/timing.csv";
                size_t emptyProjects = 0;
                size_t existingProjects = 0;
                size_t validProjects = 0;
                size_t errorProjects = 0;
                DownloaderTimingsLoader(timings, [&](std::string const & user, std::string const & repo, unsigned commits) {
                        if (commits == 0) {
                            ++emptyProjects;
                            return;
                        }
                        std::cerr << "Loading project " << user << "/" << repo << std::endl;
                        Project * p = CreateProject(user, repo);
                        if (p == nullptr) {
                            ++existingProjects;
                            return;
                        }
                        try {
                            p->loadCommits(path);
                            if (!p->commits.empty()) {
                                p->filterMasterBranch();
                                p->ignoreSubmodules(path);
                                p->removeEmptyCommits();
                                //p->compactCommitHierarchy();
                                p->write();
                                ++validProjects;
                            } else {
                                ++emptyProjects;
                            }
                        } catch (char const * e) {
                            std::cout << helpers::escapeQuotes(user) << "," << helpers::escapeQuotes(repo) << "," << helpers::escapeQuotes(e) << std::endl;
                            ++errorProjects;
                        } catch (std::exception const & e) {
                            std::cout << helpers::escapeQuotes(user) << "," << helpers::escapeQuotes(repo) << "," << helpers::escapeQuotes(e.what()) << std::endl;
                            ++errorProjects;
                        }
                        delete p;
                    });
                std::cerr << "Done:" << std::endl;
                std::cerr << "    " << errorProjects << " error projects" << std::endl;
                std::cerr << "    " << emptyProjects << " empty projects" << std::endl;
                std::cerr << "    " << existingProjects << " existing projects" << std::endl;
                std::cerr << "    " << validProjects << " valid projects" << std::endl;
            }

            static Project * CreateProject(std::string const & name, std::string const & repo) {
                std::string mangledName = Project::MangleName(name, repo);
                // transform the mangled name to lowercase for keeping - we can't change the user & repo just yet because the grabber is case sensitive
                std::transform(mangledName.begin(), mangledName.end(), mangledName.begin(), ::tolower);
                auto i = completedProjects_.find(mangledName);
                if (i != completedProjects_.end())
                    return nullptr;
                unsigned pid = completedProjects_.size();
                completedProjects_.insert(mangledName);
                return new Project(pid, name, repo);
            }

        protected:

            friend class Commit;
            friend class Project;
            friend class SubmoduleInfo;

            static unsigned GetHashId(std::string const & hash) {
                auto i = hashToId_.find(hash);
                if (i == hashToId_.end())
                    return UNKNOWN_HASH;
                else
                    return i->second;
            }

            static unsigned GetOrCreateHashId(std::string const & hash) {
                auto i = hashToId_.find(hash);
                if (i == hashToId_.end()) {
                    i = hashToId_.insert(std::make_pair(hash, hashToId_.size())).first;
                    // write the hash
                    hashes_ << i->second << "," << hash << std::endl;
                }
                return i->second;
            }

            static unsigned GetOrCreatePathId(std::string const & path) {
                auto i = pathToId_.find(path);
                if (i == pathToId_.end()) {
                    i = pathToId_.insert(std::make_pair(path, pathToId_.size())).first;
                    // write the path
                    paths_ << i->second << "," << helpers::escapeQuotes(path) << std::endl;
                }
                return i->second;
            }

            static unsigned GetOrCreateUserId(std::string const & email) {
                auto i = userToId_.find(email);
                if (i == userToId_.end()) {
                    i = userToId_.insert(std::make_pair(email, userToId_.size())).first;
                    // write the email
                    users_ << i->second << "," << helpers::escapeQuotes(email) << std::endl;
                }
                return i->second;
            }
            


            /** Global map from SHA1 hashes used by github to ids used internally. When object's hash is in this map, the object does not have to be processed. 
             */
            static std::unordered_map<std::string, unsigned> hashToId_;
            static std::ofstream hashes_;
            
            /** Global map of paths so that we can convert them to ids in the output data.
             */
            static std::unordered_map<std::string, unsigned> pathToId_;
            static std::ofstream paths_;

            /** Global map from user emails to their ids.
             */
            static std::unordered_map<std::string, unsigned> userToId_;
            static std::ofstream users_;

            /** Set of projects already finished.

                TODO this does not work for incremental updates, but incremental updates should probably be done differently?
             */
            static std::unordered_set<std::string> completedProjects_;

            static std::unordered_set<unsigned> seenCommits_;
            
        }; // ProjectAnalyzer


        
        std::unordered_map<std::string, unsigned> ProjectAnalyzer::hashToId_;
        std::ofstream ProjectAnalyzer::hashes_;
        std::unordered_map<std::string, unsigned> ProjectAnalyzer::pathToId_;
        std::ofstream ProjectAnalyzer::paths_;
        std::unordered_map<std::string, unsigned> ProjectAnalyzer::userToId_;
        std::ofstream ProjectAnalyzer::users_;
        std::unordered_set<std::string> ProjectAnalyzer::completedProjects_;
        std::unordered_set<unsigned> ProjectAnalyzer::seenCommits_;
        
        Commit::Commit(std::string const & hash, std::string const & authorEmail, uint64_t authorTime, std::string const & committerEmail, uint64_t committerTime, std::string const & tag):
            id(0),
            hash(hash),
            authorEmail(authorEmail),
            authorTime(authorTime),
            committerEmail(committerEmail),
            committerTime(committerTime) {
            if (! tag.empty())
                tags.insert(tag);
        }

        inline void Project::loadCommits(std::string const & path) {
            std::cerr << "    commits ... ";
            std::string filename = getPath(path + "/commit_metadata") + ".csv";
            DownloaderCommitMetadataLoader{filename, [this](std::string const & hash, std::string const & authorEmail, uint64_t authorTime, std::string const & committerEmail, uint64_t committerTime, std::string const & tag) {
                    Commit * c = new Commit(hash, authorEmail, authorTime, committerEmail, committerTime, tag);
                    commits.insert(std::make_pair(hash, c));
                    commits_.insert(c);
                    if (c->authorTime < this->createdAt)
                        this->createdAt = c->authorTime;
                }};
            std::cerr << std::flush;
            std::cerr << "    commit parents ... ";
            filename = getPath(path + "/commit_parents") + ".csv";
            DownloaderCommitParentsLoader{filename, [this](std::string const & commitHash, std::string const & parentHash){
                    assert(commits.find(commitHash) != commits.end());
                    Commit * c = commits[commitHash];
                    assert(commits.find(parentHash) != commits.end());
                    Commit * p = commits[parentHash];
                    c->parents.insert(p);
                    p->children.insert(c);
                }};
            /*            
            std::cerr << "    commit messages ... ";
            filename = getPath(path + "/commit_comments") + ".csv";
            DownloaderCommitMessagesLoader{filename, [this](std::string const & commitHash, std::string const & message){
                    assert(commits.find(commitHash) != commits.end());
                    Commit * c = commits[commitHash];
                    c->message = message;
                }};
            */
            std::cerr << "    file changes ... ";
            unsigned validChanges = 0;
            filename = getPath(path + "/commit_file_hashes") + ".csv";
            DownloaderCommitChangesLoader{filename, [& validChanges, this](std::string const & commitHash, std::string const & fileHash, char changeType, std::string const & path, std::string const & path2) {
                    assert(commits.find(commitHash) != commits.end());
                    Commit * c = commits[commitHash];
                    if (changeType == 'R' || changeType == 'C') {
                        assert(&path2 != & DownloaderCommitChangesLoader::NOT_A_RENAME);
                        if (ProjectAnalyzer::IsValidPath(path2)) { // if source is valid path, emit delete of the source
                            unsigned pathId = ProjectAnalyzer::GetOrCreatePathId(path2);
                            c->addChange(pathId, FILE_DELETED);
                        }
                    } else {
                        assert(&path2 == & DownloaderCommitChangesLoader::NOT_A_RENAME);
                    }
                    if (ProjectAnalyzer::IsValidPath(path)) {
                        unsigned pathId = ProjectAnalyzer::GetOrCreatePathId(path);
                        unsigned contentsId = ProjectAnalyzer::GetOrCreateHashId(fileHash);
                        c->addChange(pathId, contentsId);
                        ++validChanges;
                    }
                }};
            std::cerr << "    " << validChanges << " of these are changes to valid files" << std::endl;

        }

        Commit * Project::getMasterHead(std::string const & expectedTag) {
            Commit * masterCommit = nullptr;
            for (auto i : commits) {
                Commit * c = i.second;
                if (c->isMasterHead(expectedTag)) {
                    assert(masterCommit == nullptr);
                    masterCommit = c;
                    break;
                }
            }
            return masterCommit;
        }

        void Project::ignoreSubmodules(std::string const & path) {
            // if there are no submodules in the project, no need to deal with them
            std::string spath = getPath(path + "/submodule_museum") + "/";
            if (! containsSubmodules_)
                return;
            std::cerr << "Removing submodule changes..." << std::flush;
            CommitForwardIterator<Project,Commit,SubmoduleInfo> it(this, [this, spath](Commit * c, SubmoduleInfo & submodules) {
                    submodules.updateWith(c, spath);
                    return true;
                });
            std::cerr << "Total commits: " << commits.size() << std::endl;
            /*
            for (auto i : commits)
                if (i.second->numParentCommits() == 0)
                    it.addInitialCommit(i.second);
            */
            it.process();
        }
        
        void Project::filterMasterBranch() {
            std::cerr << "    filtering master branch only ... ";
            unsigned gitmodulesId = ProjectAnalyzer::GetOrCreatePathId(".gitmodules");
            Commit * masterCommit = getMasterHead("origin/HEAD");
            if (masterCommit == nullptr)
                masterCommit = getMasterHead("HEAD -> HEAD");
            assert(masterCommit != nullptr);
            // now we have master commit, tag all commits visible from master
            std::unordered_set<Commit *> masterCommits;
            std::vector<Commit *> q;
            q.push_back(masterCommit);
            while (! q.empty()) {
                Commit * c = q.back();
                q.pop_back();
                if (masterCommits.find(c) != masterCommits.end())
                    continue;
                for (Commit * p : c->parents)
                    q.push_back(p);
                // check if the commit changes any submodule information, if it does the commits contains submodules and we must deal with them
                if (containsSubmodules_ == false)
                    for (auto i : c->changes) {
                        if (i.first == gitmodulesId) {
                            containsSubmodules_ = true;
                            break;
                        }
                    }
                masterCommits.insert(c);
            }
            // now remove all commits that are not in master commits
            for (auto i = commits.begin(), e = commits.end(); i != e; ) {
                Commit * c = i->second;
                if (masterCommits.find(c) == masterCommits.end()) {
                    c->detachFromHierarchy();
                    delete c;
                    i = commits.erase(i);
                } else {
                    ++i;
                }
            }
            std::cerr << commits.size() << " commits left" << std::endl;
        }

        void Project::removeEmptyCommits() {
            std::cerr << "    removing empty commits ... ";
            std::vector<std::string> toBeDeleted;
            for (auto i : commits) {
                Commit * c = i.second;
                if (c->changes.empty()) {
                    c->detachFromHierarchy();
                    toBeDeleted.push_back(i.first);
                    delete c;
                }
            }
            std::cerr << toBeDeleted.size() << " empty commits found, ";
            for (std::string const & c : toBeDeleted) {
                commits.erase(c);
            }
            std::cerr << commits.size() << " commits left" << std::endl;
        }

        /** Writes the project to the output files.

            Since file hash to id and path to id outputs were already created when the input data was read, we only output file changes, commits and the project info itself this time. 
         */
        void Project::write() {
            {
                std::ofstream commitTimes(DataDir.value() + "/commits.csv", std::ios_base::app);
                //std::ofstream commitMessages(DataDir.value() + "/commitMessages.csv", std::ios_base::app);
                std::ofstream commitAuthors(DataDir.value() + "/commitAuthors.csv", std::ios_base::app);
                std::ofstream commitParents(DataDir.value() +"/commitParents.csv", std::ios_base::app);
                for (auto i : commits) {
                    Commit * c = i.second;
                    c->id = ProjectAnalyzer::GetOrCreateHashId(c->hash);
                    if (ProjectAnalyzer::seenCommits_.find(c->id) != ProjectAnalyzer::seenCommits_.end())
                        continue;
                    // commit id, authorTime, committerTime
                    commitTimes << c->id << "," << c->authorTime << "," << c->committerTime << std::endl;
                    // commit id, message
                    //commitMessages << c->id << "," << helpers::escapeQuotes(c->message) << std::endl;
                    // commit id, authorId, committerId
                    commitAuthors << c->id << "," << ProjectAnalyzer::GetOrCreateUserId(c->authorEmail) << "," << ProjectAnalyzer::GetOrCreateUserId(c->committerEmail) << std::endl;
                }
                for (auto i : commits) {
                    Commit * c = i.second;
                    if (ProjectAnalyzer::seenCommits_.find(c->id) != ProjectAnalyzer::seenCommits_.end())
                        continue;
                    ProjectAnalyzer::seenCommits_.insert(c->id);
                    for (Commit * p: c->parents)
                        commitParents << c->id << "," << p->id << std::endl;
                }
            }
            {
                std::ofstream changes(DataDir.value() + "/fileChanges.csv", std::ios_base::app);
                assert(changes.good());
                for (auto i : commits) {
                    for (auto ch : i.second->changes) {
                        // project, commit, path, hash
                        changes << id << "," << i.second->id << "," << ch.first << "," << ch.second << std::endl;
                    }
                }
            }
            {
                std::ofstream projects(DataDir.value() + "/projects.csv", std::ios_base::app);
                // now we can switch to lowercase because all data has been reade from case sensitive downloader
                std::transform(user.begin(), user.end(), user.begin(), ::tolower);
                std::transform(repo.begin(), repo.end(), repo.begin(), ::tolower);
                // pid, user, repo
                projects << id << "," << helpers::escapeQuotes(user) << "," << helpers::escapeQuotes(repo) << "," << createdAt << std::endl;
            }
            
        }



        void SubmoduleInfo::updateWith(Commit * c, std::string const & path) {
            // first see if there is a change to gitmodules
            unsigned pathId = ProjectAnalyzer::GetOrCreatePathId(".gitmodules");
            for (auto i = c->changes.begin(), e = c->changes.end(); i != e; ++i) {
                if (i->first == pathId) {
                    // if there is submodules, move all existing submodules into pending delete submodules
                    for (auto i : submodules)
                        submodulesPendingDelete.insert(i);
                    // analyze the new version of submodules
                    if (i->second != FILE_DELETED) {
                        std::ifstream f(path + c->hash);
                        std::string line;
                        while (std::getline(f, line)) {
                            size_t x = line.find("path = ");
                            if (x == std::string::npos)
                                continue;
                            line = line.substr(x + 7);
                            // std::cout << line << " -- " << c->hash << std::endl;
                            unsigned pathId = ProjectAnalyzer::GetOrCreatePathId(line);
                            submodules.insert(pathId);
                            // check that the submodule is not in the pending deletes and remove it if so
                            auto j = submodulesPendingDelete.find(pathId);
                            if (j != submodulesPendingDelete.end())
                                submodulesPendingDelete.erase(j);
                        }
                    }
                    // if we see a change to gitmodules, we can stop, there can be only one change per commit
                    c->changes.erase(i);
                    break;
                }
            }
            // now process the file changes, ignoring anything that is in submodules, and anything that is in 
            for (auto i = c->changes.begin(); i != c->changes.end(); ) {
                if (submodules.find(i->first) != submodules.end()) {
                    //std::cout << "Removing change to file " << i->pathId << std::endl;
                    i = c->changes.erase(i);
                } else if (submodulesPendingDelete.find(i->first) != submodulesPendingDelete.end()) {
                    submodulesPendingDelete.erase(i->first);
                    i = c->changes.erase(i);
                } else {
                    ++i;
                }
            }
            /*


            
            // first ignore all deletes to files we recognize as submodules - this is important in case commit removes submodule (shown as delete of a file previously known as submodule and a change to gitmodules) and then create normal file of the same name as the submodule used to be
            for (auto i = c->changes.begin(); i != c->changes.end(); ) {
                if (i->second == FILE_DELETED && submodules.find(i->first) != submodules.end()) {
                    i = c->changes.erase(i);
                } else {
                    ++i;
                }
            }
            //now see if the commit changes gitmodule file, which means we should update our submodules information to reflect that of the gitmodules change
            // now that we know the submodules delete any access to a submodule 
            for (auto i = c->changes.begin(); i != c->changes.end(); ) {
                if (submodules.find(i->first) != submodules.end()) {
                    //std::cout << "Removing change to file " << i->pathId << std::endl;
                    i = c->changes.erase(i);
                } else {
                    ++i;
                }
                } */
        }
        
        
    } // anonymous namespace


    
    void Join(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(DownloaderDir);
        Settings.addOption(TempDir);
        Settings.parse(argc, argv);
        Settings.check();
        // initializes the project analyzer and loads the existing hashes
        ProjectAnalyzer::Initialize();
        // if the downloader dir contains the timing.csv file then it is single extracted chunk 
        if (helpers::FileExists(DownloaderDir.value() + "/timing.csv"))
            ProjectAnalyzer::Analyze(DownloaderDir.value());
        // otherwise we assume it contains chunks and extract & join all of them
        else 
            ProjectAnalyzer::AnalyzeDir();
    }
    
} // namespace dejavu
