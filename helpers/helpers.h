#pragma once

#include <sstream>

#define STR(WHAT) static_cast<std::stringstream &&>(std::stringstream() << WHAT).str()
#define STRLN(WHAT) static_cast<std::stringstream &&>(std::stringstream() << WHAT << std::endl).str()
