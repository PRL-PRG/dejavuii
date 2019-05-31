#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <string>

#include "helpers/strings.h"

#include "../commands.h"
#include "../loaders.h"
#include "../commit_iterator.h"

namespace dejavu {

    namespace {

        class Filter {
        public:
            void filter() {
                
            }
            

            
        }; 


        
    } // anonymous namespace
    


    void TimeSubset(int argc, char * argv[]) {
        Settings.addOption(DataDir);
        Settings.addOption(OutputDir);
        Settings.addOption(Pct);
        Settings.parse(argc, argv);
        Settings.check();

        Filter f;
        f.filter();
        
    }
    
} // nameaspace dejavu
