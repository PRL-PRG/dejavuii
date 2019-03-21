#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace dejavu {

    void combine_and_output(std::string output_path,
                            std::unordered_map<std::string, unsigned> &project_ids,
                            std::vector<std::string> &project_infos);

    void
    fill_project_id_map(std::unordered_map<std::string, unsigned> &project_ids);

}