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



        class Project {
        public:
            static std::string MangleName(std::string const & user, std::string const & repo) {
                return user + "_" + repo;
            }

            std::string user_;
            std::string repo_;
            std::string name_;

            std::unordered_set<std::string> commitComments_;
            std::unordered_set<std::string> commitFileHashes_;
            std::unordered_set<std::string> commitMetadata_;
            std::unordered_set<std::string> commitParents_;

            Project(std::string const & user, std::string const & repo):
                user_(user),
                repo_(repo) {
                name_ = MangleName(user, repo);
            }

            std::string getPath(std::string const & where) {
                if (user_.size() >= 3) 
                    return STR(where << "/" << name_.substr(0,3) << "/" << name_);
                else
                    return STR(where << "/" << user_ << "/" << name_);
            }

            /** Loads commits metadata for the given project.
             */
            void loadCommits(std::string const & path);

            size_t sumPairs(std::initializer_list<std::pair<size_t, size_t>> pairs) {
                size_t result = 0;
                for (auto i : pairs) 
                    result = result + i.first + i.second;
                return result;
            }

            bool verify();

            std::pair<size_t, size_t> verifyCommitSets(std::unordered_set<std::string> const & first, std::unordered_set<std::string> const & second);

        };

        /** Verifies the basic statistics from the downloaded data. 
         */
        class Verifier {
        public:

            static bool IsValidPath(std::string const & path) {
                return helpers::endsWith(path, ".js") || helpers::endsWith(path, ".json") || path == ".gitmodules";
            }

            //            tar -zxf repos-10.tar.gz -C /home/peta/xxxxxx
            static void VerifyDir() {
                std::cerr << "Verifying directory " << DownloaderDir.value() << std::endl;
                helpers::DirectoryReader(DownloaderDir.value(), [](std::string const & filename) {
                        if (! helpers::endsWith(filename,".tar.gz")) {
                            std::cerr << "Skipping file " << filename << std::endl;
                            return;
                        }
                        std::cerr << "Verifying file " << filename << std::endl;
                        helpers::TempDir t(TempDir.value());
                        std::cerr << "Decompressing " << filename << " into " << t.path() << "..." << std::endl;
                        helpers::System(STR("tar -zxf " << filename << " -C " << t.path()));
                        std::cerr << "Verifying the chunk..." << std::endl;
                        Verify(t.path());
                    });
            }


            static void Verify(std::string const & path) {
                std::string timings = path + "/timing.csv";
                size_t projects = 0;
                size_t emptyProjects = 0;
                size_t invalidProjects = 0;
                size_t errorProjects = 0;
                DownloaderTimingsLoader(timings, [&](std::string const & user, std::string const & repo, unsigned commits) {
                        ++projects;
                        if (commits == 0) {
                            ++emptyProjects;
                            return;
                        }
                        //std::cerr << "Loading project " << user << "/" << repo << std::endl;
                        Project p(user, repo);
                        p.loadCommits(path);
                        try {
                            p.loadCommits(path);
                            if (! p.verify())
                                ++invalidProjects;
                        } catch (std::string const & e) {
                            std::cerr << e << std::endl;
                            ++errorProjects;
                        }
                    });
                std::cerr << "Done:" << std::endl;
                std::cerr << "    " << projects << " total projects" << std::endl;
                std::cerr << "    " << emptyProjects << " empty projects" << std::endl;
                std::cerr << "    " << errorProjects << " error projects" << std::endl;
                std::cerr << "    " << invalidProjects << " invalid projects" << std::endl;
            }


        protected:

            friend class Commit;
            friend class Project;
            friend class SubmoduleInfo;
        }; // ProjectVerifier
        
        void Project::loadCommits(std::string const & path) {
            //            std::cerr << "    metadata ... ";
            std::string filename = getPath(path + "/commit_metadata") + ".csv";
            DownloaderCommitMetadataLoader{filename, [this](std::string const & hash, std::string const & authorEmail, uint64_t authorTime, std::string const & committerEmail, uint64_t committerTime, std::string const & tag) {
                    if (hash.size() != 40)
                        throw STR("Invalid hash: " << hash);
                    commitMetadata_.insert(hash);
                }};
            //std::cerr << std::flush;
            //std::cerr << "    commit parents ... ";
            filename = getPath(path + "/commit_parents") + ".csv";
            DownloaderCommitParentsLoader{filename, [this](std::string const & commitHash, std::string const & parentHash) {
                    if (commitHash.size() != 40)
                        throw STR("Invalid hash: " << commitHash);
                    if (parentHash.size() != 40)
                        throw STR("Invalid hash: " << parentHash);
                    commitParents_.insert(commitHash);
                    commitParents_.insert(parentHash);
                }};
            //std::cerr << "    commit messages ... ";
            filename = getPath(path + "/commit_comments") + ".csv";
            DownloaderCommitMessagesLoader{filename, [this](std::string const & commitHash, std::string const & message){
                    if (commitHash.size() != 40)
                        throw STR("Invalid hash: " << commitHash);
                    commitComments_.insert(commitHash);
                }};
            //std::cerr << "    file changes ... ";
            unsigned validChanges = 0;
            filename = getPath(path + "/commit_file_hashes") + ".csv";
            DownloaderCommitChangesLoader{filename, [& validChanges, this](std::string const & commitHash, std::string const & fileHash, char changeType, std::string const & path, std::string const & path2) {
                    if (commitHash.size() != 40)
                        throw STR("Invalid hash: " << commitHash);
                    if (fileHash.size() != 40)
                        throw STR("Invalid file hash: " << fileHash);
                    commitFileHashes_.insert(commitHash);
                }};
        }

        bool Project::verify() {
            auto commentsFileHashes = verifyCommitSets(commitComments_, commitFileHashes_);
            auto commentsMetadata = verifyCommitSets(commitComments_, commitMetadata_);
            auto commentsParents = verifyCommitSets(commitComments_, commitParents_);
            auto fileHashesMetadata = verifyCommitSets(commitFileHashes_, commitMetadata_);
            auto fileHashesParents = verifyCommitSets(commitFileHashes_, commitParents_);
            auto metadataParents = verifyCommitSets(commitMetadata_, commitParents_);
            size_t diff = sumPairs({commentsFileHashes, commentsMetadata, commentsParents, fileHashesMetadata, fileHashesParents, metadataParents});
            if (diff != 0) {
                std::cout << helpers::escapeQuotes(user_) << "," << helpers::escapeQuotes(repo_) << "," << diff  << ","
                          << commentsFileHashes.first << "," << commentsFileHashes.second << ","
                    << commentsMetadata.first << "," << commentsMetadata.second << ","
                    << commentsParents.first << "," << commentsParents.second << ","
                    << fileHashesMetadata.first << "," << fileHashesMetadata.second << ","
                    << fileHashesParents.first << "," << fileHashesParents.second << ","
                    << metadataParents.first << "," << metadataParents.second << std::endl;
                return false;
            } else {
                return true;
            }
        }

        std::pair<size_t, size_t> Project::verifyCommitSets(std::unordered_set<std::string> const & first, std::unordered_set<std::string> const & second) {
            size_t missingA = 0;
            for (auto i : first) {
                if (second.find(i) == second.end())
                    ++missingA;
            }
            size_t missingB = 0;
            for (auto i : second) {
                if (first.find(i) == first.end())
                    ++missingB;
            }
            return std::make_pair(missingA, missingB);
        }

    } // anonymous namespace


    
    void Verify(int argc, char * argv[]) {
        Settings.addOption(DownloaderDir);
        Settings.addOption(TempDir);
        Settings.parse(argc, argv);
        Settings.check();
        // if the downloader dir contains the timing.csv file then it is single extracted chunk 
        if (helpers::FileExists(DownloaderDir.value() + "/timing.csv"))
            Verifier::Verify(DownloaderDir.value());
        // otherwise we assume it contains chunks and extract & join all of them
        else 
            Verifier::VerifyDir();
    }
    
} // namespace dejavu
