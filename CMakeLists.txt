cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For clang-tidy.
set(BUILD_SHARED_LIBS OFF) # We expect external libraries to be linked statically.
set(CMAKE_CXX_STANDARD 17) # Compile as C++17.
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Require C++17 support.

project(MiniBusTub
        VERSION 2023.1
        DESCRIPTION "The BusTub Relational Database Management System (Educational) @ https://github.com/cmu-db/bustub"
        LANGUAGES C CXX
)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to `Debug` as none was specified.")
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# People keep running CMake in the wrong folder, completely nuking their project or creating weird bugs.
# This checks if you're running CMake from a folder that already has CMakeLists.txt.
# Importantly, this catches the common case of running it from the root directory.
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)

if(EXISTS "${PATH_TO_CMAKELISTS_TXT}")
    message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\" \
    Some junk files were created in this folder (CMakeCache.txt, CMakeFiles); you should delete those.")
endif()

# Expected directory structure.
set(BUSTUB_BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build_support")
set(BUSTUB_CLANG_SEARCH_PATH "/usr/local/bin" "/usr/bin" "/usr/local/opt/llvm/bin" "/usr/local/opt/llvm@14/bin"
        "/opt/homebrew/opt/llvm@14/bin/")

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION MATCHES "^14.")
        message(STATUS "You're using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
    else()
        message(WARNING "!! We recommend that you use clang-14 for developing BusTub. You're using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}, a different version.")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    message(STATUS "You're using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
else()
    message(WARNING "!! We recommend that you use clang-14 for developing BusTub. You're using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}, which is not clang.")
endif()

# #####################################################################################################################
# DEPENDENCIES
# #####################################################################################################################

# CTest
enable_testing()

# clang-format

# attempt to find the binary if user did not specify
find_program(CLANG_FORMAT_BIN
        NAMES clang-format clang-format-14
        HINTS ${BUSTUB_CLANG_SEARCH_PATH})

if("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(WARNING "BusTub/main couldn't find clang-format.")
else()
    message(STATUS "BusTub/main found clang-format at ${CLANG_FORMAT_BIN}")
endif()

# attempt to find the binary if user did not specify
find_program(CLANG_TIDY_BIN
        NAMES clang-tidy clang-tidy-14
        HINTS ${BUSTUB_CLANG_SEARCH_PATH})

if("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    message(WARNING "BusTub/main couldn't find clang-tidy.")
else()
    # Output compile_commands.json
    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
    message(STATUS "BusTub/main found clang-tidy at ${CLANG_TIDY_BIN}")
endif()

find_program(CLANG_APPLY_REPLACEMENTS_BIN
        NAMES clang-apply-replacements clang-apply-replacements-14
        HINTS ${BUSTUB_CLANG_SEARCH_PATH})

if("${CLANG_APPLY_REPLACEMENTS_BIN}" STREQUAL "CLANG_APPLY_REPLACEMENTS_BIN-NOTFOUND")
    message(WARNING "BusTub/main couldn't find clang-apply-replacements.")
else()
    # Output compile_commands.json
    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
    message(STATUS "BusTub/main found clang-apply-replacements at ${CLANG_APPLY_REPLACEMENTS_BIN}")
endif()

# cpplint
find_program(CPPLINT_BIN
        NAMES cpplint cpplint.py
        HINTS "${BUSTUB_BUILD_SUPPORT_DIR}")

if("${CPPLINT_BIN}" STREQUAL "CPPLINT_BIN-NOTFOUND")
    message(WARNING "BusTub/main couldn't find cpplint.")
else()
    message(STATUS "BusTub/main found cpplint at ${CPPLINT_BIN}")
endif()

# #####################################################################################################################
# COMPILER SETUP
# #####################################################################################################################
if(NOT DEFINED BUSTUB_SANITIZER)
    set(BUSTUB_SANITIZER address)
endif()

message(STATUS "Build mode: ${CMAKE_BUILD_TYPE}")
message(STATUS "${BUSTUB_SANITIZER} sanitizer will be enabled in debug mode.")

# Compiler flags.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-unused-parameter -Wno-attributes") # TODO: remove
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fsanitize=${BUSTUB_SANITIZER} -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG: ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_EXE_LINKER_FLAGS: ${CMAKE_EXE_LINKER_FLAGS}")
message(STATUS "CMAKE_SHARED_LINKER_FLAGS: ${CMAKE_SHARED_LINKER_FLAGS}")

# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Includes.
set(BUSTUB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(BUSTUB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(BUSTUB_THIRD_PARTY_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/third_party
        ${PROJECT_SOURCE_DIR}/third_party/fmt/include
        ${PROJECT_SOURCE_DIR}/third_party/libpg_query/include
        ${PROJECT_SOURCE_DIR}/third_party/argparse/include
        ${PROJECT_SOURCE_DIR}/third_party/cpp_random_distributions
        ${PROJECT_SOURCE_DIR}/third_party/backward-cpp/include
)

include_directories(${BUSTUB_SRC_INCLUDE_DIR} ${BUSTUB_TEST_INCLUDE_DIR} ${BUSTUB_THIRD_PARTY_INCLUDE_DIR})
include_directories(BEFORE src) # This is needed for gtest.

function(disable_target_warnings NAME)
    target_compile_options(${NAME} PRIVATE "-w")
endfunction()

# #####################################################################################################################
# Other CMake modules
# MUST BE ADDED AFTER CONFIGURING COMPILER PARAMETERS
# #####################################################################################################################
set(CMAKE_MODULE_PATH "${BUSTUB_BUILD_SUPPORT_DIR}/cmake;${CMAKE_MODULE_PATH}")
find_package(LibElf)
find_package(LibDwarf)

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(tools)

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make format"
# "make check-format"
# #########################################
string(CONCAT BUSTUB_FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
)

# Runs clang format and updates files in place.
add_custom_target(format ${BUSTUB_BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUSTUB_BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${BUSTUB_FORMAT_DIRS}
        --fix
        --quiet
)