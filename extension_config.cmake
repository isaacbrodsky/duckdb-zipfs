# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
set(ZIPFS_WASM_LINKED_LIBS
    "../../vcpkg_installed/wasm32-emscripten/lib/libminiz.a"
    "../../vcpkg_installed/wasm32-emscripten/lib/libz.a")

duckdb_extension_load(zipfs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    LINKED_LIBS "${ZIPFS_WASM_LINKED_LIBS}"
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
