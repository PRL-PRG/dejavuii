#include <iostream>
#include <vector>
#include <unordered_set>
#include <thread>
#include <mutex>

#include "../loaders.h"
#include "../commands.h"

namespace dejavu {

    namespace {


        class FileSpec {
        public:
            unsigned projectId;
            unsigned commitId;
            unsigned pathId;
            unsigned contentsId;
        };

        
        class ContentsDownloader {
        public:

            void loadData() {
                {
                    std::cerr << "Loading file contents to load..." << std::endl;
                    size_t numRequests = 0;
                    std::string empty;
                    FileChangeLoader{Input.value(), [&, this](unsigned projectId, unsigned commitId, unsigned pathId, unsigned contentsId){
                            ++numRequests;
                            if (! contents_.insert(contentsId).second)
                                return;
                            filesToDownload_.push_back(FileSpec{projectId, commitId, pathId, contentsId});
                            projects_.insert(std::make_pair(projectId, empty));
                            commits_.insert(std::make_pair(commitId, empty));
                            paths_.insert(std::make_pair(pathId, empty));
                        }};
                    std::cerr << "    " << numRequests << " download requests" << std::endl;
                    std::cerr << "    " << filesToDownload_.size() << " unique files to download" << std::endl;
                }
                {
                    std::cerr << "Getting project urls..." << std::endl;
                    ProjectLoader{[this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                            if (projects_.find(id) != projects_.end())
                                projects_[id] = user + "/" + repo;
                        }};
                }
                {
                    std::cerr << "Getting commits ..." << std::endl;
                    HashToIdLoader{[this](unsigned id, std::string const & hash){
                            if (commits_.find(id) != commits_.end())
                                commits_[id] = hash;
                        }};
                }
                {
                    std::cerr << "Getting paths..." << std::endl;
                    PathToIdLoader{[&,this](unsigned id, std::string const & path){
                            if (paths_.find(id) != paths_.end())
                                paths_[id] = path;
                        }};
                }
            }

            void download() {
                std::cerr << "Downloading contents..." << std::endl;
                std::vector<std::thread> threads;
                size_t completed = 0;
                size_t errors = 0;
                size_t existing = 0;
                size_t notFound = 0;
                for (unsigned stride = 0; stride < NumThreads.value(); ++stride)
                    threads.push_back(std::thread([&, this]() {
                        while (true) {
                            size_t i = 0;
                            {
                                std::lock_guard<std::mutex> g(m_);
                                if (completed == filesToDownload_.size())
                                    return;
                                i = completed;
                                ++completed;
                                if (completed % 100 == 0)
                                    std::cerr << " : " << completed << ", existing: " << existing << ", not found: " << notFound << ", errors: " << errors << "    \r" << std::flush;
                            }
                            FileSpec const & f = filesToDownload_[i];
                            unsigned result = downloadFile(f);
                            {
                                std::lock_guard<std::mutex> g(m_);
                                switch (result) {
                                case NOT_FOUND:
                                    ++notFound;
                                    break;
                                case EXISTS:
                                    ++existing;
                                    break;
                                case ERROR:
                                    ++errors;
                                }
                            }
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << " : " << completed << ", existing: " << existing << ", not found: " << notFound << ", errors: " << errors << "    " << std::endl;
            }
            


        private:

            static unsigned constexpr DOWNLOADED = 0;
            static unsigned constexpr EXISTS = 1;
            static unsigned constexpr NOT_FOUND = 2;
            static unsigned constexpr ERROR = 3;

            /** Returns the file in which the contents should be stored
             */
            std::string getContentsFile(unsigned contentsId) {
                return STR(OutputDir.value() << "/" << (contentsId % 1000) << "/" << contentsId);
            }
            std::string getContentsFolder(unsigned contentsId) {
                return STR(OutputDir.value() << "/" << (contentsId % 1000));
            }

            
            /** Downloads the given file.
             */
            unsigned downloadFile(FileSpec const & f) {
                std::string filename = getContentsFile(f.contentsId);
                // if the file already exists, it has been downloaded by some other run before, no need to redownload
                if (helpers::FileExists(filename))
                    return EXISTS;
                // otherwise download the file now
                std::string url = std::string("https://raw.githubusercontent.com/" + projectStr(f.projectId) + "/" + commitHash(f.commitId) + "/" + pathStr(f.pathId));


                std::string contents = helpers::Exec(STR("curl -s " << url), "");
                if (contents == "404: Not Found\n")
                    return NOT_FOUND;
                // invalid structure
                if (contents.find("{") == std::string::npos)
                    return ERROR;
                // save the contents of the file
                helpers::EnsurePath(getContentsFolder(f.contentsId));
                std::ofstream out(filename);
                out << contents;
                return DOWNLOADED;
            }


            std::string const & projectStr(unsigned projectId) {
                auto i = projects_.find(projectId);
                assert(i != projects_.end());
                return i->second;
            }
            
            std::string const & commitHash(unsigned commitId) {
                auto i = commits_.find(commitId);
                assert(i != commits_.end());
                return i->second;
            }
            
            std::string const & pathStr(unsigned pathId) {
                auto i = paths_.find(pathId);
                assert(i != paths_.end());
                return i->second;
            }

            std::vector<FileSpec> filesToDownload_;

            std::unordered_map<unsigned, std::string> projects_;
            std::unordered_map<unsigned, std::string> commits_;
            std::unordered_map<unsigned, std::string> paths_;
            std::unordered_set<unsigned> contents_;

            std::mutex m_;
            
        }; // ContentsDownloader



        
    } // anonymous namespace
    


    void DownloadContents(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.addOption(Input);
        Settings.addOption(NumThreads);
        Settings.parse(argc, argv);
        Settings.check();


        helpers::EnsurePath(OutputDir.value());
        ContentsDownloader cd;
        cd.loadData();
        cd.download();


        
    }
    
} // namespace dejavu
