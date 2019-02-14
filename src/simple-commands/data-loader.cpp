#include "simple-commands.h"

#include "helpers/csv-reader.h"
#include "helpers/strings.h"


#include "../settings.h"
#include "../objects.h"

namespace dejavu {

    namespace {
        helpers::Option<std::string> InputDir("inputDir", "/filtered", false);


        class ChangesLoader : public FileChange::Reader {

        protected:

            void onRow(unsigned projectId, unsigned pathId, unsigned fileHashId, unsigned commitId) override {
                Project * p = Project::Get(projectId);
                File * f = p->getFile(Path::Get(pathId));
                Commit * c = Commit::Get(commitId);
                f->changes.insert(File::Change{c, fileHashId});
                FileHash * fh = FileHash::Get(fileHashId);
                fh->projects.insert(p);
            }

            void onDone(size_t numRows) override {
                
            }


            
        }; 

        
    } // anonymous namespace

    void DataLoader(int argc, char * argv[]) {
        settings.addOption(DataRoot);
        settings.addOption(InputDir);
        settings.parse(argc, argv);
        settings.check();

        Project::ImportFrom(DataRoot.value() + InputDir.value() +"/projects.csv", true);
        Commit::ImportFrom(DataRoot.value() + InputDir.value() + "/commits.csv", true);
        Path::ImportFrom(DataRoot.value() + InputDir.value() + "/paths.csv");
        FileHash::ImportFrom(DataRoot.value() + InputDir.value() + "/snapshots.csv", true);
        ChangesLoader cl;
        cl.readFile(DataRoot.value() + InputDir.value() + "/files.csv");
    }

    
} // namespace dejavu
