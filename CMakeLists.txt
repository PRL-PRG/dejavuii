project(dejavu)
cmake_minimum_required(VERSION 2.8)


SET( EX_PLATFORM 64 )
SET( EX_PLATFORM_NAME "x64" )

add_definitions(
  -std=c++11 -Wall -O2
  #-std=c++11 -Wall -g
)

set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

find_package(Threads)
#find_package(Boost REQUIRED COMPONENTS system)


#include_directories(${Boost_INCLUDE_DIR})
include_directories(".")

file(GLOB_RECURSE SRC "src/*.cpp" "src/*.h")

add_executable(dejavu ${SRC})
target_link_libraries(${PROJECT_NAME} ${CMAKE_THREAD_LIBS_INIT})
#target_link_libraries(${PROJECT_NAME} ${Boost_LIBRARIES} rt)
target_link_libraries(${PROJECT_NAME} OpenSSL::Crypto)
