cmake_minimum_required(VERSION 3.16)
project(driver_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

if (NOT MSVC)
    add_compile_options(-Wall -Wextra -Wno-unused-parameter -O2)
endif()

# unixODBC is the only system dependency. We deliberately do NOT link
# against libadbc_driver_manager because the ADBC bridge exports a
# stable AdbcDriverInit symbol and dlopen + AdbcDriverInit is sufficient
# to drive it from a public bench harness without adding an apt-install
# step for end users.
find_path(ODBC_INCLUDE_DIR
    NAMES sql.h
    HINTS /usr/include /usr/local/include /opt/homebrew/include
    PATH_SUFFIXES "")
find_library(ODBC_LIBRARY
    NAMES odbc
    HINTS /usr/lib /usr/lib/x86_64-linux-gnu /usr/local/lib /opt/homebrew/lib)
if (NOT ODBC_INCLUDE_DIR OR NOT ODBC_LIBRARY)
    message(FATAL_ERROR
        "unixODBC headers / library not found. Install with:\n"
        "  Debian/Ubuntu: sudo apt-get install -y unixodbc unixodbc-dev\n"
        "  RHEL/Fedora  : sudo dnf install -y unixODBC unixODBC-devel\n"
        "  macOS        : brew install unixodbc")
endif()
message(STATUS "ODBC include: ${ODBC_INCLUDE_DIR}")
message(STATUS "ODBC library: ${ODBC_LIBRARY}")

add_executable(driver_bench
    src/bench_main.cpp
    src/odbc_path.cpp
    src/adbc_path.cpp
)

target_include_directories(driver_bench
    PRIVATE
    ${ODBC_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(driver_bench
    PRIVATE
    ${ODBC_LIBRARY}
    ${CMAKE_DL_LIBS}            # dlopen / dlsym for the ADBC path
)
