#pragma once

#include "../objects.h"
#include "../loaders.h"
#include "../commit_iterator.h"

namespace dejavu {

    class Commit : public BaseCommit<Commit> {
    public:
        Commit(unsigned id, uint64_t time):
            BaseCommit<Commit>(id, time) {
        }
    };

    class Project : public BaseProject<Project, Commit> {
    public:
        Project(unsigned id, uint64_t createdAt):
            BaseProject<Project, Commit>(id, createdAt) {
        }
    };

    class Dir;

    class PathSegments {
    public:
        unsigned getIndex(std::string name) {
            auto i = helper_.find(name);
            if (i == helper_.end()) {
                unsigned id = pathSegments_.size();
                pathSegments_.push_back(name);
                i = helper_.insert(std::make_pair(name, id)).first;
            }
            return i->second;
        }

        void save(std::string const & filename) {
            std::ofstream psegs(filename);
            psegs << "#segmentId,str" << std::endl;
            for (size_t i = 0, e = pathSegments_.size(); i < e; ++i)
                psegs << i << "," << helpers::escapeQuotes(pathSegments_[i]) << std::endl;
        }

        void load() {
            PathSegmentsLoader{[this](unsigned id, std::string const & str) {
                    if (id >= pathSegments_.size())
                        pathSegments_.resize(id + 1);
                    pathSegments_[id] = str;
                    helper_.insert(std::make_pair(str, id));
                }};
        }

        void clearHelpers() {
            helper_.clear();
        }

        size_t size() const {
            return pathSegments_.size();
        }

        std::string const & operator [] (size_t index) const {
            assert(index < pathSegments_.size());
            return pathSegments_[index];
        }

        PathSegments() {
            pathSegments_.push_back("");
            helper_.insert(std::make_pair("",EMPTY_PATH));
        }
    private:
        std::vector<std::string> pathSegments_;
        std::unordered_map<std::string, unsigned> helper_;
    };

    /** Describes a file in either the global tree, or project level tree.

        Contains pathId of the path the file represents and its name.

        Note that folder clone original finder repurposes the pathId as contents id.

        TODO or not ??
     */
    class File {
    public:
        unsigned pathId;
        unsigned name;
        Dir * parent;
            
        File(unsigned pathId, unsigned name, Dir * parent);

        ~File();
    };

    class Dir {
    public:
        unsigned name;
        Dir * parent;
        std::unordered_map<unsigned, Dir *> dirs;
        std::unordered_map<unsigned, File *> files;


        size_t numFiles() const {
            size_t result = files.size();
            for (auto i : dirs) 
                result += i.second->numFiles();
            return result;
        }

        std::string path(PathSegments const & pathSegments) const {
            if (parent == nullptr)
                return "";
            std::string ppath = parent->path(pathSegments);
            if (ppath.empty())
                return pathSegments[name];
            else
                return ppath + "/" + pathSegments[name];
        }

        bool empty() const {
            return dirs.empty() && files.empty();
        }


        File * addPath(unsigned id, std::string const & path, PathSegments & pathSegments) {
            assert(parent == nullptr && "files can only be added to root dirs");
            // split the path into the folders
            std::vector<std::string> p = helpers::Split(path, '/');
            Dir * d = this;
            for (size_t i = 0; i + 1 < p.size(); ++i) // for all directories
                d = d->getOrCreateDirectory(pathSegments.getIndex(p[i]));
            return new File(id, pathSegments.getIndex(p.back()), d);
        }

        void fillFrom(Dir * other) {
            for (auto i : other->dirs) {
                assert(i.first == i.second->name);
                Dir * d = new Dir(i.first, this);
                d->fillFrom(i.second);
            }
            for (auto i : other->files) {
                assert(i.first == i.second->name);
                new File(i.second->pathId, i.first, this);
            }
        }

        Dir(unsigned name, Dir * parent):
            name(name),
            parent(parent) {
            if (parent != nullptr) {
                assert(parent->dirs.find(name) == parent->dirs.end());
                parent->dirs.insert(std::make_pair(name, this));
            }
        }
            
        ~Dir() {
            if (parent != nullptr)
                parent->dirs.erase(name);
            while (!dirs.empty())
                delete dirs.begin()->second;
            while (!files.empty())
                delete files.begin()->second;
        }

    private:

        File * createFile(unsigned pathId, unsigned name) {
            assert(files.find(name) == files.end());
            File * f = new File(pathId, name, this);
            files.insert(std::make_pair(name, f));
            return f;
        }
            
        Dir * getOrCreateDirectory(unsigned name) {
            auto i = dirs.find(name);
            if (i != dirs.end())
                return i->second;
            return new Dir(name, this);
        }
    };

    inline File::File(unsigned pathId, unsigned name, Dir * parent):
        pathId(pathId),
        name(name),
        parent(parent) {
        assert(parent != nullptr);
        assert(parent->files.find(name) == parent->files.end());
        parent->files.insert(std::make_pair(name, this));
    }

        
    inline File::~File() {
        if (parent != nullptr)
            parent->files.erase(name);
    }

    
    
}
