#pragma once
#include <functional>
#include <unordered_map>

#include "helpers/csv-reader.h"
#include "helpers/hash.h"

#include "settings.h"

namespace dejavu {

    constexpr unsigned UNKNOWN_HASH = -1;
    constexpr unsigned FILE_DELETED = 0;

    class BaseLoader : public helpers::CSVReader {
    public:
        void readFile(std::string const & filename) {
            // we always have headers
            parse(filename, true);
            onDone(numRows());
        }

    protected:

        /** Called when the requested file has been all read.
         */
        virtual void onDone(size_t n) {
            std::cerr << n << " records loaded" << std::endl;
        }
        
    }; // dejavuii::BaseLoader



    /** Loads the table which translates hashes to their ids.
     */
    class HashToIdLoader : public BaseLoader {
    public:
        typedef std::function<void(unsigned, std::string const &)> RowHandler;
        
        HashToIdLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        HashToIdLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/hashes.csv");
        }

        HashToIdLoader(std::string const & filename, std::unordered_map<std::string, unsigned> & transitions):
            f_([&](unsigned id, std::string const & path) {
                    transitions.insert(std::make_pair(path, id));
                }) {
            readFile(filename);
        }

    protected:

        void row(std::vector<std::string> & row) override {
            unsigned id = std::stoul(row[0]);
            f_(id, row[1]);
        }

    private:
        RowHandler f_;
        
    };

    /** Loads the projects basic information.
     */
    class ProjectLoader : public BaseLoader {
    public:
        // id, user, repo, createdAt (oldest commit)
        typedef std::function<void(unsigned, std::string const &, std::string const &, uint64_t)> RowHandler;
        ProjectLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }
        ProjectLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/projects.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 4);
            unsigned id = std::stoul(row[0]);
            uint64_t createdAt = std::stoull(row[3]);
            f_(id, row[1], row[2], createdAt);
        }

    private:
        RowHandler f_;
    };

    /** Loads the commits basic information.
     */
    class CommitLoader : public BaseLoader {
    public:
        // id, authorTime, committerTime
        typedef std::function<void(unsigned, uint64_t, uint64_t)> RowHandler;

        CommitLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        CommitLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/commits.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 3);
            unsigned id = std::stoul(row[0]);
            uint64_t authorTime = std::stoull(row[1]);
            uint64_t committerTime = std::stoull(row[1]);
            f_(id, authorTime, committerTime);
        }

    private:
        RowHandler f_;
        
    };
    /** Loads the commits information about parents.
     */
    class CommitParentsLoader : public BaseLoader {
    public:
        // id, parentId
        typedef std::function<void(unsigned, unsigned)> RowHandler;

        CommitParentsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        CommitParentsLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/commitParents.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 2);
            unsigned id = std::stoul(row[0]);
            unsigned parentId = std::stoul(row[1]);
            f_(id, parentId);
        }

    private:
        RowHandler f_;
        
    };

    /** Loads the table which translates paths to their ids.
     */
    class PathToIdLoader : public BaseLoader {
    public:
        typedef std::function<void(unsigned, std::string const &)> RowHandler;
        
        PathToIdLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        PathToIdLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/paths.csv");
        }

        PathToIdLoader(std::string const & filename, std::unordered_map<std::string, unsigned> & transitions):
            f_([&](unsigned id, std::string const & path) {
                    transitions.insert(std::make_pair(path, id));
                }) {
            readFile(filename);
        }

    protected:

        void row(std::vector<std::string> & row) override {
            unsigned id = std::stoul(row[0]);
            f_(id, row[1]);
        }

    private:
        RowHandler f_;
        
    };

    /** Loads the file change records.
     */
    class FileChangeLoader : public BaseLoader {
    public:
        // project id, commit id, path id, contents id
        typedef std::function<void(unsigned, unsigned, unsigned, unsigned)> RowHandler;

        FileChangeLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FileChangeLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/fileChanges.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 4);
            unsigned projectId = std::stoul(row[0]);
            unsigned commitId = std::stoul(row[1]);
            unsigned pathId = std::stoul(row[2]);
            unsigned contentId = std::stoul(row[3]);
            f_(projectId, commitId, pathId, contentId);
        }

    private:
        RowHandler f_;
        
    }; 

    /** Loads the user email to id translation table.
     */
    class UsersLoader : public BaseLoader {
    public:
        typedef std::function<void(unsigned, std::string const &)> RowHandler;

        UsersLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

    protected:
        void row(std::vector<std::string> & row) override {
            f_(std::stoul(row[0]), row[1]);
        }

    private:

        RowHandler f_;
    };

    /** Loads the information in timing.csv files produced by the downloader identifying the projects correctly downloaded.
     */
    class DownloaderTimingsLoader : public BaseLoader {
    public:
        typedef std::function<void(std::string const &, std::string const &, unsigned)> RowHandler;

        DownloaderTimingsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

    protected:

        void row(std::vector<std::string> & row) override {
            f_(row[1], row[2], std::stoul(row[6]));
        }

    private:

        RowHandler f_;
    };

    /** Loads the commit metadata information as obtained by the downloader.
     */
    class DownloaderCommitMetadataLoader : public BaseLoader {
    public:
        typedef std::function<void(std::string const &, std::string const &, uint64_t, std::string const &, uint64_t, std::string const &)> RowHandler;
        DownloaderCommitMetadataLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }
    protected:

        void row(std::vector<std::string> & row) override {
            if (row.size() != 6) 
                throw "Invalid metadata row size";
            if (row[2].empty() || row[4].empty())
                throw "Empty author or commit time";
            try {
                uint64_t authorTime = std::stoull(row[2]);
                uint64_t committerTime = std::stoull(row[4]);
                f_(row[0], row[1], authorTime, row[3], committerTime, row[5]);
            } catch (...) {
                throw "Error while parsing author or committer time";
            }
        }

    private:

        RowHandler f_;
    };

    /** Downloads the commit parents information as obtained by the downloader.
      */
    class DownloaderCommitParentsLoader : public BaseLoader {
    public:
        typedef std::function<void(std::string const &, std::string const &)> RowHandler;
        DownloaderCommitParentsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }
    protected:

        void row(std::vector<std::string> & row) override {
            assert(row.size() == 2);
            f_(row[0], row[1]);
        }

    private:

        RowHandler f_;
    };

    /** Downloads the commit messages as reported by the downloader.
     */
    class DownloaderCommitMessagesLoader : public BaseLoader {
    public:
        typedef std::function<void(std::string const &, std::string const &)> RowHandler;
        DownloaderCommitMessagesLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }
    protected:

        void row(std::vector<std::string> & row) override {
            if (row.size() != 2)
                std::cerr << row.size() << " -- " << row[0] << std::endl;
            assert(row.size() == 2);
            f_(row[0], row[1]);
        }

    private:

        RowHandler f_;
    }; 

    /** Downloads the commit changes as reported by the downloader.
        
        The handler function takes 5 arguments, the last argument is either identical to NOT_A_RENAME (address-wise), or contains the target file of the rename if the current change is a rename. 
     */
    class DownloaderCommitChangesLoader : public BaseLoader {
    public:
        typedef std::function<void(std::string const &, std::string const &, char, std::string const &, std::string const &)> RowHandler;
        DownloaderCommitChangesLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        static std::string const NOT_A_RENAME;

    protected:

        void row(std::vector<std::string> & row) override {
            if (row.size() != 5)
                throw "invalid num of rows";
            if (row[4].empty())
                f_(row[0], row[1], row[2][0], row[3], NOT_A_RENAME);
            else
                f_(row[0], row[1], row[2][0], row[3], row[4]);
        }

    private:

        RowHandler f_;
    };
    

    



       



    
} // namespace dejavuii
