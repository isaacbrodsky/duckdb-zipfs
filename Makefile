PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=zipfs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

coverage:
	EXT_DEBUG_FLAGS=-DENABLE_COVERAGE=1 make debug
	cmake --build build/debug --config Debug --target clean-coverage
	make test_debug
	cmake --build build/debug --config Debug --target coverage
