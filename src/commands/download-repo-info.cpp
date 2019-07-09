#include <stdio.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <thread>
#include <mutex>
#include <src/commit_iterator.h>
#include <sstream>
#include <chrono>

#include <curl/curl.h>

#include "../loaders.h"
#include "helpers/json.hpp"

namespace dejavu {

    struct Repository {
        std::string user;
        std::string project;

        bool operator ==(const Repository &r) const noexcept {
            return (user == r.user) && (project == r.project);
        }
    };

    struct RepositoryHash {
        size_t operator ()(const Repository &r) const noexcept {
            return std::hash<std::string>()(r.user) << 32
                   | std::hash<std::string>()(r.project);
        }
    };

    struct RepositoryComp {
        bool operator()(const Repository &r1, const Repository &r2) const noexcept {
            return r1.project == r2.project
                   && r1.user == r2.user;
        }
    };

    void LoadRepositories(std::unordered_set<Repository, RepositoryHash, RepositoryComp> &repositories) {
        clock_t timer = clock();
        size_t total_repositories = 0;
        std::string task = "loading repository user and project names";
        helpers::StartTask(task, timer);

        auto f = [&](std::string const &user, std::string const &project) {
            ++total_repositories;
            Repository repo;
            repo.user = user;
            repo.project = project;
            repositories.insert(repo);
        };

        std::vector<std::string> paths = helpers::Split(RepositoryList.value(), ':');

        std::cerr << "Given " << paths.size()
                  << " file paths to read repositories from"
                  << std::endl;

        for (std::string const &path : paths) {
            std::cerr << "Loading repositories from " << path << std::endl;

            RepositoryListLoader loader = RepositoryListLoader(path, f);

            std::cerr << "Lines skipped: "
                      << loader.getSkipped()
                      << std::endl;
            std::cerr << "Repositories loaded: "
                      << total_repositories
                      << std::endl;
            std::cerr << "Unique repositories loaded: "
                      << repositories.size()
                      << std::endl;
        }

        helpers::FinishTask(task, timer);
    }

    struct CurlMetaData {
        long status = 0;
        std::string status_message;
        long rate_limit = 0;
        long rate_limit_remaining = 0;
        long rate_limit_reset = 0;
        int header_read_fields = 0;
    };

    struct DownloadData {
        std::stringstream out;
    };

    size_t DataCallback(void *data, size_t size, size_t elements, void *user_data) {
        DownloadData *download_data = (DownloadData *) user_data;
        std::string s = (char *) data;
        download_data->out << s;
        return size * elements;
    }

    void ParseHttpHeaderProperty(std::string content, CurlMetaData* user_data) {
        std::vector<std::string> cols = helpers::Split(content, ':', 2);
        std::string value = helpers::strip(cols[1]);
//        for (std::string s : cols)
//            std::cerr << "    >> " << s << std::endl;

        if (cols[0] == "Status") {
            std::vector<std::string> e = helpers::Split(value, ' ', 2);
            user_data->status = std::stoul(e[0]);
            user_data->status_message = helpers::strip(e[1]);
        } else if (cols[0] == "X-RateLimit-Limit") {
            user_data->rate_limit = std::stoul(value);
        } else if (cols[0] == "X-RateLimit-Remaining") {
            user_data->rate_limit_remaining = std::stoul(value);
        } else if (cols[0] == "X-RateLimit-Reset") {
            user_data->rate_limit_reset = std::stoul(value);
        } else {
            // Ignore.
        }
    }

    size_t HeaderCallback(char *data, size_t size, size_t elements, void *user) {
        CurlMetaData* user_data = (CurlMetaData *) user;
        std::string content = data;

        if (user_data->header_read_fields > 0 && helpers::strip(content) != "") {
            ParseHttpHeaderProperty(content, user_data);
        }

        ++user_data->header_read_fields;
        return size * elements;
    }

    void DownloadOne(std::string const &url, CurlMetaData &metadata, DownloadData &data) {

        CURL *curl = curl_easy_init();

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, DataCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "Deja Vu Agent");
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &data);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &metadata);

        if (GitHubPersonalAccessToken.value() != "") {
            curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
            curl_easy_setopt(curl, CURLOPT_USERPWD, GitHubPersonalAccessToken.value().c_str());
        }

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != 0) {
            ERROR("CURL ERROR " << res);
        }
    }

    void SaveOne(Repository const &repository, DownloadData const &data) {
        std::string path = DataDir.value()
                           + "/repository_details/"
                           + repository.user + "_"
                           + repository.project + ".json";

        std::ofstream file(path);
        if (!file.good()) {
            ERROR("Unable to open file " << path << " for writing");
        }

        file << data.out.str();
        file.close();
    }

    void Download(std::unordered_set<Repository, RepositoryHash, RepositoryComp> const &repository_set) {
        clock_t timer = clock();
        std::string task = STR("downloading repository info to json files for "
                                       << repository_set.size()
                                       << " repos");
        size_t good = 0;
        size_t bad = 0;
        size_t attempts = 0;
        helpers::StartTask(task, timer);

        std::ofstream good_file(DataDir.value() + "/repository_details/__downloaded.csv");
        if (!good_file.good()) {
            ERROR("Unable to open file /repository_details/__downloaded.csv for writing");
        }

        std::ofstream bad_file(DataDir.value() + "/repository_details/__failed.csv");
        if (!bad_file.good()) {
            ERROR("Unable to open file /repository_details/__failed.csv for writing");
        }

        std::vector<Repository> repositories(repository_set.begin(),
                                             repository_set.end());

        if (repositories.size() != 0) {
            Repository repository = repositories.back();
            repositories.pop_back();

            for (;;) {
                CurlMetaData metadata;
                DownloadData data;

                DownloadOne("https://api.github.com/repos/"
                            + repository.user + "/"
                            + repository.project,
                            metadata, data);

                std::cerr << metadata.status << std::endl
                          << metadata.status_message << std::endl
                          << metadata.rate_limit << std::endl
                          << metadata.rate_limit_remaining << std::endl
                          << metadata.rate_limit_reset << std::endl << std::endl;

                //std::cerr << data.out.str() << std::endl;

                if (metadata.status == 200) {
                    SaveOne(repository, data);

                    ++good;
                    //if (good % 1000 == 0 || bad % 1000 == 0) {
                    std::cerr << " : " << good << " good and "
                              << bad << " failed out of "
                              << repository_set.size() << "k\r"
                              << std::flush;
                    //}

                    good_file << repository.user << "/" << repository.project
                              << std::endl;

                    if (repositories.size() == 0) {
                        break;
                    }

                    Repository repository = repositories.back();
                    repositories.pop_back();

                } else if (metadata.status == 403
                           && metadata.rate_limit_remaining == 0) {

                    const auto t =
                            std::chrono::system_clock::from_time_t(
                                    metadata.rate_limit_reset);

                    std::time_t same_time =
                            std::chrono::system_clock::to_time_t(t);

                    std::cerr << " : waiting until " << std::ctime(&same_time)
                              //<< metadata.rate_limit_reset
                              << "\r" << std::flush;

                    std::this_thread::sleep_until(t);

                    // Do not remove, we'll try this one again.
                } else {
                    ++bad;
                    //if (good % 1000 == 0 || bad % 1000 == 0) {
                    std::cerr << " : " << good << " good and "
                              << bad << " failed out of "
                              << repository_set.size() << "k\r"
                              << std::flush;
                    //}

                    bad_file << repository.user << "/" << repository.project
                             << "," << metadata.status
                             << "," << metadata.status_message
                             << std::endl;

                    if (repositories.size() == 0) {
                        break;
                    }

                    Repository repository = repositories.back();
                    repositories.pop_back();
                }

                ++attempts;
            }
        }

        std::cerr << good << " good downloads ouf of "
                  << repository_set.size()
                  << "                                "
                  << std::endl;
        std::cerr << bad << " failed downloads ouf of "
                  << repository_set.size()
                  << std::endl;
        std::cerr << attempts << " attempted downloads out of "
                  << repository_set.size()
                  << std::endl;

        bad_file.close();
        good_file.close();

        //helpers::FinishCounting(counter);
        helpers::FinishTask(task, timer);
    }

    void DownloadRepositoryInfo(int argc, char * argv[]) {
        // Something to do with settings.
        Settings.addOption(RepositoryList);
        Settings.addOption(DataDir);
        Settings.addOption(NumThreads);
        Settings.addOption(GitHubPersonalAccessToken);
        Settings.parse(argc, argv);
        Settings.check();

        std::unordered_set<Repository, RepositoryHash, RepositoryComp> repositories;
        LoadRepositories(repositories);

        Download(repositories);
    }
};
