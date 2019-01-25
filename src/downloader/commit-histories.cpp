#include <cassert>

#include <fstream>

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>

#include "helpers/helpers.h"
#include "helpers/csv-reader.h"
#include "helpers/strings.h"
#include "../settings.h"

#include "downloader.h"

namespace dejavu {


    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/processed", false);
        helpers::Option<unsigned> NumWorkers("numWorkers", 16, false);
        helpers::Option<std::string> OutputDir("outputDir", "/processed", {"-o"}, false);
        helpers::Option<std::string> TempDir("tmp", "./tmp", false);


        class Downloader {
        public:
            Downloader(std::string const & output, std::string const & index):
                outputSize_(0) {
                output_.open(output);
                index_.open(index);
                if (! output_.good())
                    throw std::runtime_error(STR("Unable to open file " << output));
                if (! index_.good())
                    throw std::runtime_error(STR("Unable to open file " << index));
            }

            void download(std::string const & projects, unsigned workers) {
                parserDone_ = false;
                workers_ = 0;
                outputSize_ = 0;
                Downloader * d = this;
                // create the reporter thread
                std::thread reporter([d] () {
                        size_t seconds = 0;
                        while (true) {
                            ++seconds;
                            std::cerr << helpers::ToTime(seconds) << " " << d->workers_ << " " << d->done_.size() << " - " << (d->done_.size() * 1.0 / seconds) << " projects per second" << std::endl;
                            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                        }
                });
                reporter.detach();
                // create the workers
                for (size_t x = 0; x != workers; ++x) {
                    std::thread t([d] () {
                            ++d->workers_;
                            d->worker();
                            --d->workers_;
                        });
                    t.detach();
                }
                // now parse the file
                ProjectsReader reader(this);
                reader.parse(projects, false);
                parserDone_ = true;
                while (workers_ != 0) 
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }

        private:

            /** Reads the projects from the dataset and issues their download & history analysis. 
             */
            class ProjectsReader : public helpers::CSVReader {
            public:
                ProjectsReader(Downloader * d):
                    downloader_(d) {
                }
            protected:
                void row(std::vector<std::string> & row) override;
            private:
                friend class Downloader;
                Downloader * downloader_;
                
            }; // ProjectsReader

            friend class ProjectsReader;

            void analyzeProject(unsigned id, std::string const & name) {
                std::string tmp = STR(TempDir.value() << "/" << id);
                try {
                    std::string cmdGit = STR("GIT_TERMINAL_PROMPT=0 git clone http://github.com/" << name << " " << tmp << " --quiet");
                    int err = system(cmdGit.c_str());
                    if (err == -1)
                        std::cerr << "Unable to download project " << id << ", " << name << std::endl;
                    // otherwise we have the project
                    std::string cmdCommits = "git log --all --pretty=\"%H %at %ct %P -- %D\"";
                    std::string result = helpers::Exec(cmdCommits, tmp);
                    storeCommitsInfo(id, std::move(result));
                } catch (...) {
                    
                }
                // remove the temporary file
                system(STR("rm -rf " << tmp).c_str());
            }

            void storeCommitsInfo(unsigned id, std::string && output) {
                std::lock_guard<std::mutex> g(outputM_);
                // first store the index into the index file
                index_ << id << "," << outputSize_ << std::endl << std::flush;
                done_[id] = outputSize_;
                // then store the actual output
                std::string header = STR("# " << id << "\n");
                output_ << header << output << std::flush;
                outputSize_ += output.size() + header.size();
            }

            void addProjectToQueue(unsigned id, std::string const & name) {
                std::unique_lock<std::mutex> g(workQueueM_);
                while (q_.size() > 100)
                    workQueueFullCv_.wait(g);
                q_.push(std::make_pair(id, name));
                workQueueReadyCv_.notify_one();
            }

            void worker() {
                while (true) {
                    unsigned id;
                    std::string repoPath;
                    {
                        std::unique_lock<std::mutex> g(workQueueM_);
                        while (q_.empty()) {
                            if (parserDone_)
                                return;
                            workQueueReadyCv_.wait(g);
                        }
                        id = q_.front().first;
                        repoPath = q_.front().second;
                        q_.pop();
                    }
                    workQueueFullCv_.notify_one();
                    analyzeProject(id, repoPath);
                }
            }

            /** Projects already upgraded.
             */
            std::unordered_map<unsigned, unsigned> done_;

            size_t outputSize_;

            std::ofstream output_;
            std::ofstream index_;

            std::queue<std::pair<unsigned, std::string>> q_;
            std::condition_variable workQueueFullCv_;
            std::condition_variable workQueueReadyCv_;
            std::mutex workQueueM_;
            std::mutex outputM_;
            std::atomic<unsigned> workers_;
            volatile bool parserDone_;
            
        }; //Downloader
    } // anonymous namespace


    void Downloader::ProjectsReader::row(std::vector<std::string> & row) {
        assert(row.size() == 3 && "Invalid number of columns for project");
        unsigned id = std::stoul(row[0]);
        std::string repoPath = row[1] + "/" + row[2];
        downloader_->addProjectToQueue(id, repoPath);
    }
    
    void DownloadCommitHistories(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.addOption(NumWorkers);
        settings.addOption(OutputDir);
        settings.addOption(TempDir);
        settings.parse(argc, argv);
        settings.check();


        std::string output = STR(DataRoot.value() + OutputDir.value() << "/commit-history.txt");
        std::string index = STR(DataRoot.value() + OutputDir.value() << "/commit-history-index.csv");
        std::string projects = STR(DataRoot.value() + InputDir.value() << "/projects.csv");

        Downloader d(output, index);
        d.download(projects, NumWorkers.value());

    }

    
} // namespace dejavu
