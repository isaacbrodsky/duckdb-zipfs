cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME zipfs)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(miniz REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LibLZMA REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/zipfs_extension.cpp src/zip_file_system.cpp src/bz2_file_system.cpp src/xz_file_system.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} miniz::miniz BZip2::BZip2 LibLZMA::LibLZMA)
target_link_libraries(${LOADABLE_EXTENSION_NAME} miniz::miniz BZip2::BZip2 LibLZMA::LibLZMA)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
