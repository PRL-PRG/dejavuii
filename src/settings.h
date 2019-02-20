#pragma once

#include "helpers/settings.h"

namespace dejavu {

    extern helpers::Option<std::string> DataRoot;
    extern helpers::Option<unsigned> Seed;
    extern helpers::Option<unsigned> NumThreads;

    extern helpers::Settings settings;
    
} // namespace dejavu
