#include "helpers/helpers.h"
#include "../settings.h"
#include "src/objects.h"
#include "helpers/sectioned-text-file-reader.h"
#include "selective-commit-network.h"

#include <sstream>
#include <unordered_map>

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed", false);
        helpers::Option<std::string> CommitsDir("commitsDir", "/processed", false);
        helpers::Option<std::string> CommitHistoryDir("commitHistoryDir", "/processed", false);
    } // anonymous namespace

    class Graph;

    class Node {
    public:
        std::string const hash;
        std::set<Node *> parents;
        std::set<Node *> children;
    private:
        Node(std::string hash) : hash(hash) {}

        friend class Graph;
    };

    class Graph {
    public:
        Node *getOrCreate(std::string hash) {
            auto it = nodes.find(hash);
            if (it == nodes.end()) {
                Node *node = new Node(hash);
                nodes[hash] = node;
                return node;
            } else {
                return it->second;
            }
        }

        void remove(Node * node) {
            // Remove the current node from all of its the parents.
            // std::cerr << ":: :: remove node from parents" << std::endl;
            for (auto p : node->parents) {
                p->children.erase(node);
            }

            // Remove the current node from all of its children.
            // std::cerr << ":: :: remove node from children" << std::endl;
            for (auto c : node->children) {
                c->parents.erase(node);
            }

            // Remove the current node from the node list
            // std::cerr << ":: :: remove node from graph" << std::endl;
            nodes.erase(node->hash);

            // Delete node.
            // std::cerr << ":: :: delete node" << std::endl;
            delete(node);
        }

//        std::vector<Edges> getEdges() {
//            int written_lines = 0;
//            for (auto it : nodes) {
//                // Boop.
//                Node * node = it.second;
//
//                // Print an edge between the node and all its parents.
//                for (auto parent : node->parents) {
//                    s << prefix
//                      << node->hash << ","
//                      << parent->hash << std::endl;
//                    written_lines++;
//                }
//            }
//            return written_lines;
//        }

        // All nodes in existence.
        std::unordered_map<std::string, Node *> nodes;
    };


    class CommitHistoryLoader : helpers::SectionedTextFileReader {
    public:
        size_t readFile(std::string const &filename) {
            std::cerr << "Importing from file " << filename << std::endl;
            first_project_ = true;
            size_t rows = parse(filename);
            std::cerr << "Total number of commits " << rows << std::endl;
            return rows;
        }

    protected:
        void section_header(std::vector<std::string> &row) override {

            assert(row.size() == 2 && "Unknown section header format: invalid number of fields.");
            assert(row[0] == "#" && "Unknown section header format: expected first field to be '#'");

            if (!first_project_) {
                onProject(project_id_);
            }

            project_id_ = std::stoi(row[1]);
            first_project_ = false;
        };

        void row(std::vector<std::string> &row) override {

            assert(row.size() > 3 && "Unknown commit data format: invalid number of fields.");

            const std::string hash = row[0];
            // std::cout << "Commit : " << hash << std::endl;

            // ignoring times for now
            //const unsigned long commit_time = std::stol(row[1]);
            //const unsigned long author_time = std::stol(row[2]);

            std::vector<std::string> parent_list;
            size_t i = 3;
            for (; i < row.size(); i++) {
                if (row[i] == "--") {
                    break;
                }

                // std::cout << "    " << row[i] << std::endl;

                // if it's not 40 it is not SHA-1 hash, but some garbage
                if (row[i].size() == 40) {
                    parent_list.push_back(row[i]);
                }
            }

            // ignoring tag for now
            //std::stringstream tag;
            //for (bool first_word=true; i < row.size(); i++, first_word=false) {
            //    if (!first_word)
            //        s << " ";
            //    tag << row[i];
            //}

            onCommit(hash, /*commit_time,*/ /*author_time,*/
                     parent_list /*, tag.str() */);
        }

        void file_end(unsigned int lines) override {
            if (!first_project_) {
                onProject(project_id_);
            }
        }

        virtual void onProject(unsigned int project_id) = 0;

        virtual void onCommit(std::string const & hash,
                              std::vector<std::string> const & parent_list) = 0;

    private:
        int project_id_;
        bool first_project_;
    };

    typedef std::function<bool(unsigned int, std::string)> CommitFilter;

    class CommitHistorySelection : public CommitHistoryLoader {
    public:
        CommitHistorySelection(CommitFilter node_filter) :
                is_node_selected(node_filter) {
            graph = new Graph();
        }

        void saveAll(std::string const &filename) {
            std::ofstream s(filename);
            if (! s.good())
                ERROR("Unable to open file " << filename << " for writing");
            int written_lines = 0;
            s << "project_id,hash,parent_hash" << std::endl;
            for (auto it : graphs) {
                // Boop.
                unsigned int project_id = it.first;
                Graph * graph = it.second;

                // Make a line to mark the project exists but has no commits.
                if (graph->nodes.empty()) {
                    s << project_id << ",NA,NA" << std::endl;
                    written_lines++;
                    continue;
                }

                for (auto it : graph->nodes) {
                    // Boop.
                    Node * node = it.second;
    
                    // Print an edge to NA if no parents.
                    if (node->parents.empty()) {
                        s << project_id << ","
                          << node->hash << ","
                          << "NA" << std::endl;
                        written_lines++;
                    }

                    // Print an edge between the node and all its parents.
                    for (auto parent : node->parents) {
                        s << project_id << ","
                          << node->hash << ","
                          << parent->hash << std::endl;
                        written_lines++;
                    }
                }
            }

            std::cerr << "Written " << written_lines << " lines to file \""
                      << filename << "\"" << std::endl;
        }

    protected:
        void onCommit(std::string const & hash,
                      std::vector<std::string> const & parent_list) override {
            assert(hash.size() == 40);

            // Create a node.
            Node * node = graph->getOrCreate(hash);

            // Connect the node to its parents and children.
            for (auto parent_hash : parent_list) {
                Node * parent = graph->getOrCreate(parent_hash);
                node->parents.insert(parent);
                parent->children.insert(node);
            }
        }

        void onProject(unsigned int project_id) override {
            std::cerr << "Processing graph for project " << project_id <<  std::endl;

            std::list<Node *> queue;
            std::unordered_set<Node *> already_scheduled;
            int initial_node_count = graph->nodes.size();

            // Find the beginning point of a topological traverse: all the nodes
            // that have no parents. There should be just one, but the algorithm
            // should work if there are more for some reason.
            for (auto it : graph->nodes) {
                Node *node = it.second;
                if (node->parents.size() == 0) {
                    assert(already_scheduled.find(node) == already_scheduled.end() 
                           && "Duplicate root in commit tree.");
                    queue.push_back(node);
                }
            }
            // std::cerr << "Found roots: " << std::endl;
            // for (auto n : queue) {
            //    std::cerr << "     " << n->hash << std::endl;
            // }
            //std::cerr << std::endl;

            // Start processing.
            // std::cerr << "Process graph (size=" << graph->nodes.size() << "): " << std::endl;
            for (auto q = queue.begin(); q != queue.end(); q++) {

                // std::cerr << "boop" << std::endl;

                // Boop.
                Node *node = *q;

                // std::cerr << "Node: " << node->hash << std::endl;
                // std::cerr << "Selected: " << is_node_selected(node->hash) << std::endl;
                // std::cerr << "Parents:" << std::endl;
                // for (auto p : node->parents) {
                //    std::cerr << "     " << p->hash << std::endl;
                // }
                // std::cerr << "Children:" << std::endl;
                // for (auto c : node->children) {
                //     std::cerr << "     " << c->hash << std::endl;
                // }

                // First, add all of this node's children to the processing
                // queue. Eventually we will traverse the entire graph in
                // topological order.
                // std::cerr << ":: push children to queue" << std::endl;
                for (auto c : node->children) {
                    if (already_scheduled.find(c) == already_scheduled.end()) {
                        already_scheduled.insert(c);
                        queue.push_back(c);
                        // std::cerr << ":: :: pushing " << c->hash << std::endl;
                    } else {
                        // std::cerr << ":: :: not pushing " << c->hash << " (already scheduled)" << std::endl;
                    }
                }

                // If the node is selected, then carry on. If not, reroute the
                // edges around it and remove it.
                if (!is_node_selected(project_id, node->hash)) {
                    // std::cerr << ":: node is not selected" << std::endl;
                    // Connect every child node with every parent node.
                    for (auto parent : node->parents)
                        for (auto child : node->children) {
                            // std::cerr << ":: connect (P) " << parent->hash
                            //           << " to (C) " << child->hash << std::endl;
                            parent->children.insert(child);
                            child->parents.insert(parent);
                        }

                    // Remove the node from the graph and from existence.
                    // std::cerr << ":: remove node" << std::endl;
                    graph->remove(node);
                } else {
                    // std::cerr << ":: node is selected, ignore" << std::endl;
                }
            }

            // Print out debug information.
            std::cerr << " : "
                      << "processed " << graphs.size() << " projects; "
                      << "currently " << project_id << " "
                      << "(" << initial_node_count
                      << " -> " << graph->nodes.size() << ")"
                      //<< "                                \r"
                      << std::endl <<  std::flush;

            // Sort out the graphs.
            graphs[project_id] = graph;
            graph = new Graph();
        }

    private:
        CommitFilter is_node_selected;
        std::unordered_map<unsigned, Graph *> graphs;

        // For communication between onCommit and onProject.
        Graph * graph;
    };

    // TODO this could be cleaned up and moved to objects
    class CommitsInProject : public helpers::CSVReader {
    public:
        size_t readFile(std::string const & filename, bool headers) {
            numRows_ = 0;
            parse(filename, headers);
            return numRows_;
        }

        static void ImportFrom(std::string const & filename, bool headers) {
            CommitsInProject cip;
            cip.readFile(filename, headers);
        }

        static bool Exists(unsigned int project_id, std::string hash) {
            //return allegiance_[project_id].find(hash) !=
            //        allegiance_[project_id].end();
        }

    protected:
        void row(std::vector<std::string> & row) override {
            assert((row.size() == 4) && "Invalid commit row length");

            unsigned project_id = std::stoul(row[0]);
            unsigned commit_id = std::stoul(row[3]);

            ++numRows_;

            Commit * commit = Commit::Get(commit_id);
            std::string hash = commit->hash;
            CommitsInProject::allegiance_[project_id].insert(hash);
        }

    private:
        size_t numRows_;
        static std::unordered_map<unsigned int, std::unordered_set<std::string>> allegiance_;
    };

    std::unordered_map<unsigned int, std::unordered_set<std::string>> CommitsInProject::allegiance_;

    void SelectiveCommitNetwork(int argc, char *argv[]) {

        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(CommitsDir);
        settings.addOption(CommitHistoryDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();

        // Import commits. This will create the set(s) used for selection.
        Commit::ImportFrom(DataRoot.value() + CommitsDir.value() + "/commits.csv", false);
        CommitsInProject::ImportFrom(DataRoot.value() + CommitsDir.value() + "/files.csv", false);

        // Import commit history and select only those that are already in the
        // commits. Re-route the edges appropriately.
        CommitHistorySelection chs(CommitsInProject::Exists);
        chs.readFile(DataRoot.value() + CommitsDir.value() + "/commit-history.txt");
        chs.saveAll(DataRoot.value() + OutputDir.value() + "/selective-commit-network.csv");
    }

} // namespace dejavu
