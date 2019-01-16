#include <iostream>

#include <boost/interprocess/managed_shared_memory.hpp>

#include "../settings.h"


namespace dejavu {

    namespace {

        helpers::Option<std::string> Name("name", "dejavuii", false);
        helpers::Option<uint64_t> Size("size", 200 * 1024 * 1024 * 1024l, false);
        
    } //anonymous namespace

    void InitializeSharedMem(int argc, char * argv []) {
        using namespace boost::interprocess;
        settings.addOption(Name);
        settings.addOption(Size);
        settings.parse(argc, argv);
        settings.check();
        std::cerr << Size.value() << std::endl;
        managed_shared_memory mem(create_only, Name.value().c_str(), Size.value());
    }

    void TerminateSharedMem(int argc, char * argv[]) {
        using namespace boost::interprocess;
        settings.addOption(Name);
        settings.parse(argc, argv);
        settings.check();
        shared_memory_object::remove(Name.value().c_str());
        
    }

    
}
