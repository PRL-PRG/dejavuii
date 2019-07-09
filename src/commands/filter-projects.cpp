#include "../objects.h"
#include "../loaders.h"
#include "../commands.h"
#include "../commit_iterator.h"

namespace dejavu {

    /** Given a list of projects and a dataset creates new dataset that would not contain data from the specified projects.

        I.e. removes all commits and file changes unique to these projects. Does not change hashes, paths, or other properties, nor does it recalculate indices.
     */
    class ProjectsFilter {
        
    }; 

    void FilterProjects(int argc, char * argv []) {
        Settings.addOption(DataDir);
        Settings.addOption(Filter);
        Settings.addOption(OutputDir);
        Settings.parse(argc, argv);
        Settings.check();
        
    }

} // namespace dejavu
