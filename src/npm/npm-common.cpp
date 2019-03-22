#include "npm-common.h"

namespace dejavu {

    void
    combine_and_output(std::string output_path,
                       std::unordered_map<std::string, unsigned> &project_ids,
                       std::vector<std::string> &projects) {

        std::ofstream csv_file(output_path);
        std::cerr << "NAO I COMBINE ALL AND WRITEZ CSV FILE TO "
                  << output_path
                  << std::endl;
        if (!csv_file.good())
            ERROR("Unable to open file " << output_path << " for writing");
        csv_file << "\"repository\",\"project_id\"" << std::endl;
        unsigned int n_projects = 0;
        for (std::string project : projects) {
            ++n_projects;

            auto it = project_ids.find(project);
            if (it != project_ids.end()) {
                unsigned project_id = it->second;
                csv_file << "\"" << project << "\"," << project_id << std::endl;
            } else {
                csv_file << "\"" << project << "\"," /* NA */ << std::endl;
            }

            if (n_projects % 1000 == 0)
                std::cerr << "    I WRITED " << n_projects / 1000
                          << "K PROJEKTZ " << "\r";
        }
        std::cerr << std::endl;
        std::cerr << "NAO I DONE " << std::endl;
    }

    void
    combine_and_output(std::string output_path,
                       std::unordered_map<std::string, unsigned> &project_ids,
                       std::vector<std::pair<std::string, std::string>> &projects) {

        std::ofstream csv_file(output_path);
        std::cerr << "NAO I COMBINE ALL AND WRITEZ CSV FILE TO "
                  << output_path
                  << std::endl;
        if (!csv_file.good())
            ERROR("Unable to open file " << output_path << " for writing");
        csv_file << "\"repository\",\"project_id\",\"url\"" << std::endl;
        unsigned int n_projects = 0;
        for (std::pair<std::string,std::string> project : projects) {
            ++n_projects;

            auto it = project_ids.find(std::get<0>(project));
            if (it != project_ids.end()) {
                unsigned project_id = it->second;
                csv_file << "\"" << std::get<0>(project) << "\"," << project_id
                         << ",\"" << std::get<1>(project) << "\"" << std::endl;
            } else {
                csv_file << "\"" << std::get<0>(project) << "\"," /* NA */
                         << ",\"" << std::get<1>(project) << "\"" << std::endl;
            }

            if (n_projects % 1000 == 0)
                std::cerr << "    I WRITED " << n_projects / 1000
                          << "K PROJEKTZ " << "\r";
        }
        std::cerr << std::endl;
        std::cerr << "NAO I DONE " << std::endl;
    }

    void
    fill_project_id_map(std::unordered_map<std::string, unsigned> &projects) {
        std::cerr << "NAO I MAK PROJEKT ID MAP" << std::endl;
        unsigned int n_projects = 0;
        for (auto project_entry : Project::AllProjects()) {
            ++n_projects;
            unsigned project_id = project_entry.first;
            std::string repository = project_entry.second->user
                                     + "/" + project_entry.second->repo;
            projects[repository] = project_id;
            if (n_projects % 1000 == 0)
                std::cerr << "    I MAPD " << n_projects / 1000
                          << "K PROJEKT IDZ " << "\r";
        }
        std::cerr << std::endl;
        std::cerr << "NAO I HAV PROJEKT ID MAP" << std::endl;
    }
}