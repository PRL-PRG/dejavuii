#include <cstdlib>
#include <iostream>

#include "helpers/helpers.h"
#include "helpers/csv-reader.h"
#include "helpers/settings.h"

#include "objects.h"

// the different commands we have available

#include "scala_join/join.h"


namespace dejavu {
    helpers::StringOption DataRoot("dataRoot", "", { "-dr"}, true);

    helpers::Settings settings;

}

using namespace dejavu;


/** A half decent main function.

 */
int main(int argc, char * argv[]) {
    std::cerr << "OH HAI!" << std::endl << "CAN I HAZ STUFFZ?" << std::endl;
    try {
        if (argc < 2)
            throw std::runtime_error("Command specification (first argument) is missing!");
        std::string cmd = argv[1];
        if (cmd == "scala-join") 
            JoinScalaChunks(argc -2, argv + 2);
        else 
            throw std::runtime_error(STR("Invalid command " << cmd));
        std::cerr << "KTHZBYE!" << std::endl;
        return EXIT_SUCCESS;
    } catch (std::runtime_error const & e) {
        std::cerr << "I HAZ DIS:" << std::endl << settings << std::endl << std::endl;
        std::cerr << "O NOES. BASEMENT CAT LURKZ IN BELGIUM. IT MEOWZ DIS:" << std::endl;
        std::cerr << e.what() << std::endl << std::endl;
        std::cerr << "Please refer to the definitely extensive and well written documentation in README.md:)" << std::endl;
        return EXIT_FAILURE;
    }
    //Project::ImportFrom(STR(DATA_FOLDER << "/processed2/projects.csv"));
    //Commit::ImportFrom(STR(DATA_FOLDER << "/processed2/commits.csv"));
    //Path::ImportFrom(STR(DATA_FOLDER << "/processed2/paths.csv"));
}
