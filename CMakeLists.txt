cmake_minimum_required(VERSION 3.5)

include(CMakeDependentOption)

# Set extension name here
set(TARGET_NAME zipfs)

project(${TARGET_NAME})

cmake_dependent_option(ENABLE_LIBARCHIVE "Enable libarchive support for additional archive formats" ON "NOT WIN32" OFF)

if (DEFINED VCPKG_TARGET_TRIPLET)
  foreach (VCPKG_PREFIX
           "${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}"
           "${CMAKE_CURRENT_LIST_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}")
    if (EXISTS "${VCPKG_PREFIX}")
      list(APPEND CMAKE_PREFIX_PATH "${VCPKG_PREFIX}")
    endif()
  endforeach()
endif()

find_package(miniz REQUIRED)
find_package(ZLIB REQUIRED)
if (ENABLE_LIBARCHIVE)
  find_package(LibArchive REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

set(EXTENSION_SOURCES
  src/zipfs_extension.cpp
  src/zip_file_system.cpp
  src/zip_member_handle.cpp
  src/deflate_index.cpp
  src/archive_file_system.cpp
  src/raw_archive_file_system.cpp
  src/noop_archive_file_system.cpp
  src/zip_contents.cpp
  src/archive_contents.cpp
  src/noop_archive_contents.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} miniz::miniz ZLIB::ZLIB)
target_link_libraries(${LOADABLE_EXTENSION_NAME} miniz::miniz ZLIB::ZLIB)
target_compile_definitions(${EXTENSION_NAME} PRIVATE MINIZ_NO_ZLIB_COMPATIBLE_NAMES)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE MINIZ_NO_ZLIB_COMPATIBLE_NAMES)
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
