cmake_minimum_required(VERSION 2.8)

SET( EX_PLATFORM 64 )
SET( EX_PLATFORM_NAME "x64" )

if (UNIX) 
    add_definitions(
        -std=c++11 -g -Wall
    )
endif (UNIX)

project(dejavuii)

include_directories("." "helpers")

file(GLOB_RECURSE SRC "src/*.cpp" "src/*.h")

add_executable(dejavuii ${SRC})
target_link_libraries( dejavuii ${LIBS} m )
