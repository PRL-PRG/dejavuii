#pragma once
#include <functional>
#include <unordered_map>
#include <limits>

#include "helpers/csv-reader.h"
#include "helpers/hash.h"

#include "objects.h"
#include "settings.h"

namespace dejavu {


    /** Determines if the given path is under 'node_modules' directory.
     */
    inline bool IsNPMPath(std::string const & p) {
        return (p.find("node_modules/") == 0) || (p.find("/node_modules/") != std::string::npos); 
    }

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
            //std::cerr << n << " records loaded" << std::endl;
        }
        
    }; // dejavuii::BaseLoader


    /** Loads projects from the GHTorrent dump.
     */
    class GHTorrentProjectsLoader : public BaseLoader {
    public:
        typedef std::function<void(unsigned, // id
                                   std::string const &, // url
                                   unsigned, // ownerId
                                   std::string const &, // name
                                   std::string const &, // description
                                   std::string const &, // language
                                   uint64_t, // createdAt
                                   unsigned, // forkedFrom
                                   uint64_t, // deleted
                                   uint64_t // updatedAt
                                   )> RowHandler;

        GHTorrentProjectsLoader(RowHandler f):
            f_(f) {
            readFile(GhtDir.value() + "/projects.csv");
        }

        GHTorrentProjectsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }
    protected:

        uint64_t strToTime(std::string const & str) {
            struct tm t;
            strptime(str.c_str(),"%Y-%m-%d %H:%M:%S",&t);
            return mktime(&t);
        }
        
        void row(std::vector<std::string> & row) override {
            // NOTE there is an 11th column, but we don't really know what that one is 
            assert(row.size() == 11);
            unsigned id = std::stoul(row[0]);
            unsigned ownerId = std::stoul(row[2]);
            uint64_t createdAt = strToTime(row[6]);
            unsigned forkedFrom = row[7] == "\\N" ? NO_FORK : std::stoul(row[7]);
            uint64_t deleted = strToTime(row[8]);
            uint64_t updatedAt = strToTime(row[9]);
            f_(id, row[1], ownerId, row[3], row[4], row[5], createdAt, forkedFrom, deleted, updatedAt);
        }

    private:
        RowHandler f_;
    };


    

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



    class PathSegmentsLoader : public BaseLoader {
    public:
        // segmentId, str
        typedef std::function<void(unsigned, std::string const &)> RowHandler;

        PathSegmentsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        PathSegmentsLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/path_segments.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 2);
            unsigned segmentId = std::stoul(row[0]);
            f_(segmentId, row[1]);
        }

    private:
        RowHandler f_;
    };


    /** Folder clone original candidate.
     */
    class FolderCloneOriginalsCandidateLoader : public BaseLoader {
    public:
        // cloneId, hash, occurences, files, projectId, commitId, path
        typedef std::function<void(unsigned, SHA1Hash const &, unsigned, unsigned, unsigned, unsigned, std::string const &)> RowHandler;

        FolderCloneOriginalsCandidateLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FolderCloneOriginalsCandidateLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/clone_originals.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 7);
            unsigned id = std::stoul(row[0]);
            SHA1Hash hash = SHA1Hash::FromHexString(row[1]);
            unsigned occurences = std::stoul(row[2]);
            unsigned files = std::stoul(row[3]);
            unsigned projectId = std::stoul(row[4]);
            unsigned cloneId = std::stoul(row[5]);
            f_(id, hash, occurences, files, projectId, cloneId, row[6]);
        }

    private:
        RowHandler f_;
    };

    class FolderCloneCandidateLoader : public BaseLoader {
    public:
        // cloneId, projectId, commitId, folder, files
        typedef std::function<void(unsigned, unsigned, unsigned, std::string const &, unsigned)> RowHandler;

        FolderCloneCandidateLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FolderCloneCandidateLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/clone_candidates.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 5);
            unsigned cloneId = std::stoul(row[0]);
            unsigned projectId = std::stoul(row[1]);
            unsigned commitId = std::stoul(row[2]);
            unsigned files = std::stoul(row[4]);
            
            f_(cloneId, projectId, commitId, row[3], files);
        }

    private:
        RowHandler f_;
    };

    class FolderCloneStructureLoader : public BaseLoader {
    public:
        // cloneId, str
        typedef std::function<void(unsigned, std::string const &)> RowHandler;

        FolderCloneStructureLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FolderCloneStructureLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/clone_strings.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 2);
            unsigned segmentId = std::stoul(row[0]);
            f_(segmentId, row[1]);
        }

    private:
        RowHandler f_;
    };

    /** Loads the folder clone occurences information.

        This info is available after filter-folder-clones command.
     */
    class FolderCloneOccurencesLoader : public BaseLoader {
    public:
        // cloneId,projectId, commitId, folder, numFiles
        typedef std::function<void(unsigned,unsigned, unsigned, std::string const &, unsigned)> RowHandler;

        FolderCloneOccurencesLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FolderCloneOccurencesLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/folderCloneOccurences.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 5);
            unsigned cloneId = std::stoul(row[0]);
            unsigned projectId = std::stoul(row[1]);
            unsigned commitId = std::stoul(row[2]);
            unsigned numFiles = std::stoul(row[4]);
            f_(cloneId, projectId, commitId, row[3], numFiles);
        }

    private:
        RowHandler f_;
        
    };

    /** Loads the folder clone originals information.

        This info is available after filter-folder-clones command.
     */
    class FolderCloneOriginalsLoader : public BaseLoader {
    public:
        // cloneId, hash, occurences, files, projectId, commitId, path, isOriginalClone
        typedef std::function<void(unsigned, SHA1Hash const &, unsigned, unsigned, unsigned, unsigned, std::string const &, bool)> RowHandler;

        FolderCloneOriginalsLoader(std::string const & filename, RowHandler f):
            f_(f) {
            readFile(filename);
        }

        FolderCloneOriginalsLoader(RowHandler f):
            f_(f) {
            readFile(DataDir.value() + "/folderCloneOriginals.csv");
        }
    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 8);
            unsigned id = std::stoul(row[0]);
            SHA1Hash hash = SHA1Hash::FromHexString(row[1]);
            unsigned occurences = std::stoul(row[2]);
            unsigned files = std::stoul(row[3]);
            unsigned projectId = std::stoul(row[4]);
            unsigned cloneId = std::stoul(row[5]);
            bool isOriginalClone = row[6] == "1";
            f_(id, hash, occurences, files, projectId, cloneId, row[6], isOriginalClone);
        }

    private:
        RowHandler f_;
        
    };

    class FileClonesLoader : public BaseLoader {
    public:
        typedef std::function<void(unsigned, unsigned, unsigned/*, std::vector<unsigned> &*/)> RowHandler;

        FileClonesLoader(std::string const & filename, RowHandler f): f_(f) {
            readFile(filename);
        }

        FileClonesLoader(RowHandler f): f_(f) {
            readFile(DataDir.value() + "/fileClusters.csv");
            //readFile(DataDir.value() + "/fileClustersWithCommits.csv");
        }

    protected:
        void row(std::vector<std::string> & row) override {
            assert(row.size() == 3);

            unsigned content_id = std::stoul(row[0]);
            unsigned cluster_size = std::stoul(row[1]);
            unsigned original_commit_id = std::stoul(row[2]);

            //std::vector<unsigned> commits;
            //for (auto & it : helpers::Split(row[3], ' ')) {
            //    commits.push_back(std::stoul(it));
            //}

            //assert(commits.size() == cluster_size);

            f_(content_id, cluster_size, original_commit_id/*, commits*/);
        }

    private:
        RowHandler f_;
    };


    
} // namespace dejavuii
