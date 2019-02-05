#include "helpers/helpers.h"
#include "../settings.h"
#include "src/objects.h"
#include "sectioned-text-file-reader.h"
#include "selective-commit-network.h"

#include <sstream>
#include <unordered_map>

namespace dejavu {

    namespace {
        helpers::Option<std::string> OutputDir("outputDir", "/processed", false);
        helpers::Option<std::string> CommitsDir("commitsDir", "/processed", false);
        helpers::Option<std::string> CommitHistoryDir("commitHistoryDir", "/processed", false);

    } // anonymous namespace

    struct Edge {
        std::string const source;
        std::string const target;
    };

    struct EdgeMeta {
        Edge edge;
        bool const source_selected;
        bool const target_selected;
    };

    class CommitHistoryLoader : helpers::SectionedTextFileReader {
    public:
        size_t readFile(std::string const &filename) {
            first_project_ = true;
            return parse(filename);
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

            // ignoring times for now
            //const unsigned long commit_time = std::stol(row[1]);
            //const unsigned long author_time = std::stol(row[2]);

            std::vector<std::string> parent_list;
            size_t i = 3;
            for (; i < row.size(); i++) {
                if (row[i] == "--")
                    break;
                parent_list.push_back(row[i]);
            }

            // ignoring tag for now
            //std::stringstream tag;
            //for (bool first_word=true; i < row.size(); i++, first_word=false) {
            //    if (!first_word)
            //        s << " ";
            //    tag << row[i];
            //}

            onCommit(hash, /*commit_time,*/ /*author_time,*/ parent_list /*, tag.str() */);
        }

        virtual void onProject(unsigned int project_id) = 0;

        virtual void onCommit(std::string hash, std::vector<std::string> const parent_list) = 0;

    private:
        int project_id_;
        bool first_project_;
    };

    class CommitHistorySelection : public CommitHistoryLoader {
    public:
        CommitHistorySelection(std::function<bool(std::string const)> node_filter) : is_node_selected(node_filter) {}

    protected:
        void onCommit(std::string hash, std::vector<std::string> parent_list) override {
            if (!is_node_selected(hash)) {

                // Step 1
                target_substitution_index[hash] = parent_list;

            } else {

                // Step 2 (simultaneous with step 1)
                for (std::string parent : parent_list) {
                    EdgeMeta edge = {{hash, parent}, true, is_node_selected(parent)};
                    temporary_edge_list.push_back(edge);
                }
            }
        }

        void onProject(unsigned int project_id) override {

            // Step 3
            for (auto it = temporary_edge_list.begin(); it != temporary_edge_list.end();) {

                if (it->target_selected) {
                    edges.push_back(it->edge);
                    temporary_edge_list.erase(it++);
                    continue;
                }

                auto sub_list_it = target_substitution_index.find(it->edge.target);
                if (sub_list_it != target_substitution_index.end()) {
                    std::vector<std::string> &sub_list = sub_list_it->second;
                    for (auto sub = sub_list.begin(); sub != sub_list.end(); sub++) {
                        EdgeMeta edge = {{it->edge.source, *sub}, true, is_node_selected(*sub)};
                        temporary_edge_list.push_back(edge);
                    }
                }

                // remove from list
                // find subs
                // each sub, add new edge

                temporary_edge_list.erase(it++);
            }
        }

        std::list<Edge> edges;

    private:
        //std::unordered_map<std::string const, std::vector<std::string const>> input_incidence_list;
        std::unordered_map<std::string, std::vector<std::string>> target_substitution_index;
        std::list<EdgeMeta> temporary_edge_list;
        std::function<bool(std::string const)> is_node_selected;
    };

    void SelectiveCommitNetwork(int argc, char *argv[]) {

        // Something to do with settings.
        settings.addOption(DataRoot);
        settings.addOption(CommitsDir);
        settings.addOption(CommitHistoryDir);
        settings.addOption(OutputDir);
        settings.parse(argc, argv);
        settings.check();

        // Import commits. This will create the set used for selection.
        Hash::ImportFrom(DataRoot.value() + CommitsDir.value() + "/commits.csv", false, 1);

        CommitHistorySelection chs([](std::string const hash){
            //Hash::
            return true;
        });
        chs.readFile(DataRoot.value() + CommitsDir.value() + "/files.csv");
        //Commit::SaveAll(DataRoot.value() + OutputDir.value() + "/commits.csv");
    }

} // namespace dejavu
