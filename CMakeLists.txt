cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME zipfs)

option(ENABLE_LIBARCHIVE "Enable libarchive support for additional archive formats" ON)

find_package(miniz REQUIRED)
if (ENABLE_LIBARCHIVE)
  find_package(LibArchive REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
  src/zipfs_extension.cpp
  src/zip_file_system.cpp
  src/archive_file_system.cpp
  src/raw_archive_file_system.cpp
  src/noop_archive_file_system.cpp
  src/zip_contents.cpp
  src/archive_contents.cpp
  src/noop_archive_contents.cpp
  src/zipfs_secret.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} miniz::miniz)
target_link_libraries(${LOADABLE_EXTENSION_NAME} miniz::miniz)
if (ENABLE_LIBARCHIVE)
  target_link_libraries(${EXTENSION_NAME} LibArchive::LibArchive)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} LibArchive::LibArchive)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE ENABLE_LIBARCHIVE=1)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE ENABLE_LIBARCHIVE=1)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

option(ENABLE_COVERAGE "Build with coverage" OFF)
if (ENABLE_COVERAGE)
    message("Coverage enabled")
    target_compile_options(${EXTENSION_NAME} PRIVATE --coverage)
    target_compile_options(${LOADABLE_EXTENSION_NAME} PRIVATE --coverage)
    target_link_options(${EXTENSION_NAME} PUBLIC --coverage)
    target_link_options(${LOADABLE_EXTENSION_NAME} PUBLIC --coverage)
    file(
            GENERATE
            OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/scripts/coverage.sh"
            INPUT "${CMAKE_CURRENT_SOURCE_DIR}/scripts/coverage.sh.in")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage.cleaned.info")
    set_property(
            DIRECTORY
            APPEND
            PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "coverage")
    add_custom_target(
            coverage
            COMMAND
            bash "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>/scripts/coverage.sh"
            "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}")
    add_custom_target(
            clean-coverage
            # Before running coverage, clear all counters
            COMMAND lcov --directory
            '${CMAKE_CURRENT_BINARY_DIR}' --zerocounters
            COMMENT "Zeroing counters")
endif()
