#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <functional>
#include <iostream>
#include <fstream>

#include "helpers/helpers.h"
#include "src/settings.h"
#include "src/objects.h"


namespace dejavu {

    void
    combine_and_output(std::string output_path,
                       std::unordered_map<std::string, unsigned> &project_ids,
                       std::vector<std::string> &projects);

    void
    combine_and_output(std::string output_path,
                       std::unordered_map<std::string, unsigned> &project_ids,
                       std::vector<std::pair<std::string, std::string>> &projects);

    void
    fill_project_id_map(std::unordered_map<std::string, unsigned> &project_ids);

}