#include <stdio.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <atomic>
#include <mutex>
#include <sstream>
#include <chrono>

#include "../loaders.h"
#include "helpers/json.hpp"



namespace dejavu {

    namespace {

        class Project {
        public:
            unsigned id;
            std::string user;
            std::string repo;
        };

        class CurlHeaders {
        public:
            long status = 0;
            std::string status_message;
            long rate_limit = 0;
            long rate_limit_remaining = 0;
            long rate_limit_reset = 0;
            int header_read_fields = 0;
            std::string location;
            
            CurlHeaders(std::string const & from) {
                for (std::string line : helpers::Split(from, '\n')) {
                    std::vector<std::string> cols = helpers::Split(line, ':', 2);
                    if (cols.size() < 2)
                        continue;
                    std::string value = helpers::strip(cols[1]);
                    if (cols[0] == "Status") {
                        std::vector<std::string> e = helpers::Split(value, ' ', 2);
                        status = std::stoul(e[0]);
                        status_message = helpers::strip(e[1]);
                    } else if (cols[0] == "X-RateLimit-Limit") {
                        rate_limit = std::stoul(value);
                    } else if (cols[0] == "X-RateLimit-Remaining") {
                        rate_limit_remaining = std::stoul(value);
                    } else if (cols[0] == "X-RateLimit-Reset") {
                        rate_limit_reset = std::stoul(value);
                    } else if (cols[0] == "Location") {
                        location = value;
                        // Ignore.
                    }
                }
            }
        };


        class DownloadManager {
        public:
            DownloadManager():
                existing_(0),
                moved_(0) {
            }
           
            void loadData() {
                std::cerr << "Loading projects ... " << std::endl;
                ProjectLoader{Input.value(), [this](unsigned id, std::string const & user, std::string const & repo, uint64_t createdAt){
                        projects_.push_back(new Project{id, user, repo});
                    }};
                std::cerr << "    " << projects_.size() << " projects to download loaded" << std::endl;
                std::cerr << "Loading authentication tokens..." << std::endl;
                StringRowLoader(GitHubPersonalAccessToken.value(), [this](std::vector<std::string> const & row) {
                        assert(row.size() == 1);
                        tokens_.push_back(row[0]);
                    });
                std::cerr << "    " << tokens_.size() << " tokens loaded" << std::endl;
            }

            void download() {
                std::vector<std::thread> threads;
                size_t completed = 0;
                size_t failed = 0;
                std::atomic<unsigned> success(0);
                for (unsigned i = 0; i < tokens_.size(); ++i)
                    threads.push_back(std::thread([i, & completed, &failed, &success, this]() {
                        while (true) {
                            Project * p ;
                            {
                                std::lock_guard<std::mutex> g(m_);
                                if (completed >= projects_.size())
                                    return;
                                p = projects_[completed];
                                ++completed;
                                if (completed % 1000 == 0)
                                    std::cerr << "     " << completed << ", failed " << failed << "    \r" << std::flush;
                            }
                            unsigned status = downloadProject(p, tokens_[i]);
                            if (status != 200) {
                                std::lock_guard<std::mutex> g(m_);
                                std::cout << p->id << "," << status << std::endl;
                                ++failed;
                            } else {
                                ++success;
                            }
                        }
                    }));
                for (auto & i : threads)
                    i.join();
                std::cerr << "     " << completed << " attempted downloads" << std::endl;
                std::cerr << "     " << existing_ << " already existing files " << std::endl;
                std::cerr << "     " << moved_ << " moved " << std::endl;
                std::cerr << "     " << failed << " failed downloads" << std::endl;
                std::cerr << "     " << success << " successful downloads" << std::endl;
            }

        private:

            unsigned downloadProject(Project * p, std::string const & authToken) {
                std::string path = STR(OutputDir.value() << "/" << (p->id % 1000) << "/" << p->id);
                // if the file has already been downloaded, skip it
                if (helpers::FileExists(path)) {
                    ++existing_;
                    return 200;
                }
                std::string url = STR("https://api.github.com/repos/" << p->user << "/" << p->repo);
                while (true) {
                    // now download the project
                    std::stringstream headersRaw;
                    std::stringstream body;
                    CURL * curl = curl_easy_init();
                    struct curl_slist *list = nullptr;
                    list = curl_slist_append(list, STR("Authorization: token " << authToken).c_str());                  
                    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
                    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlCallback);
                    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, CurlCallback);
                    curl_easy_setopt(curl, CURLOPT_USERAGENT, "DejaVuII");
                    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
                    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &headersRaw);
                    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, list);
                    /* CURLcode result = */ curl_easy_perform(curl);
                    curl_easy_cleanup(curl);
                    curl_slist_free_all(list);
                    CurlHeaders headers(headersRaw.str());
                    // if we have 200, all is fine, save the result
                    if (headers.status == 200) {
                        std::string targetDir = STR(OutputDir.value() << "/" << (p->id % 1000));
                        helpers::EnsurePath(targetDir);
                        std::ofstream f(path);
                        f << body.str();
                        return 200;
                    } else if (headers.status == 301) {
                        // retry with new location
                        ++moved_;
                        url = headers.location;
                        path = path + ".renamed";
                    } else if (headers.status == 403 && headers.rate_limit_remaining == 0) {
                        std::this_thread::sleep_until(std::chrono::system_clock::from_time_t(headers.rate_limit_reset));
                    } else {
                        return headers.status;
                    }
                }
            }

            


            static size_t CurlCallback(char * data, size_t, size_t size, void * stream) {
                static_cast<std::stringstream*>(stream)->write(data, size);
                return size;
            }

            
            std::vector<Project *> projects_;
            std::vector<std::string> tokens_;

            std::mutex m_;
            std::atomic<unsigned> existing_;
            std::atomic<unsigned> moved_;
           
        };
        
    } // anonymous namespace

    

    void DownloadGithubMetadata(int argc, char * argv[]) {
        Settings.addOption(Input);
        Settings.addOption(OutputDir);
        Settings.addOption(GitHubPersonalAccessToken);
        Settings.parse(argc, argv);
        Settings.check();

        DownloadManager dm;
        dm.loadData();
        dm.download();
    }
    
} // namespace dejavu
