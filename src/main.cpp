#include <cstdlib>
#include <iostream>
#include <functional>

#include "helpers/helpers.h"
#include "helpers/csv-reader.h"
#include "helpers/settings.h"

#include "objects.h"

// the different commands we have available

#include "simple-commands/simple-commands.h"
#include "scala-join/join.h"
#include "nm-filter/nmfilter.h"
#include "import-clones/import-clones.h"


namespace dejavu {
    helpers::Option<std::string> DataRoot("dataRoot", "", { "-dr"}, true);

    helpers::Settings settings;

}

using namespace dejavu;


typedef std::function<void(int, char * [])> CommandHandler;


struct Command {
    CommandHandler handler;
    std::string help;

};

std::unordered_map<std::string, Command> commands;

void AddCommand(std::string name, CommandHandler handler, std::string const & help) {
    assert(commands.find(name) == commands.end() && "Command already exists");
    commands[name] = Command{handler, help};
}

void InitializeCommands() {
    AddCommand("scala-join", JoinScalaChunks, "Joins the chunked verbose data downloaded by the original scala downloader into one file per type (files, projects, commits, etc.) and compresses all hashes into unique ids.");
    AddCommand("sort-files", SortFiles, "Sorts the files.csv in increasing order of commit timestamps. This is a prerequisite for the ");
    AddCommand("filter-nm", RemoveNodeModules, "Removes all files in node_modules subdirectories as well as any commits, contents, or projects that only happen in these paths");
    AddCommand("import-clones", ImportClones, "Analyzes import clones in the dataset. Work in progress");
}

/** A half decent main function.

 */
int main(int argc, char * argv[]) {
    InitializeCommands();
    std::cerr << "OH HAI!" << std::endl << "CAN I HAZ STUFFZ?" << std::endl;
    try {
        if (argc < 2)
            throw std::runtime_error("Command specification (first argument) is missing!");
        std::string cmd = argv[1];
        auto i = commands.find(cmd);
        if (i == commands.end())
            throw std::runtime_error(STR("Invalid command " << cmd));
        std::cerr << "I CAN HAZ " << cmd << " NAO:" << std::endl;
        i->second.handler(argc -2, argv + 2);
        std::cerr << "KTHXBYE!" << std::endl;
        return EXIT_SUCCESS;
    } catch (std::runtime_error const & e) {
        std::cerr << "I HAZ DIS:" << std::endl << settings << std::endl << std::endl;
        std::cerr << "O NOES. BASEMENT CAT LURKZ IN BELGIUM. IT MEOWZ DIS:" << std::endl;
        std::cerr << e.what() << std::endl << std::endl;
        std::cerr << "Please refer to the definitely extensive and well written documentation in README.md:)" << std::endl;
        return EXIT_FAILURE;
    }
}
