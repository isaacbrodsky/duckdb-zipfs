#define DUCKDB_EXTENSION_MAIN

#include "zipfs_extension.hpp"
#include "zip_file_system.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

void ZipfsExtension::Load(DuckDB &db) {
  std::string description = "Support for reading files from zip archives";
  ExtensionUtil::RegisterExtension(*db.instance, "zipfs", {description});

  auto &fs = db.instance->GetFileSystem();
  fs.RegisterSubSystem(make_uniq<ZipFileSystem>());

  auto &config = DBConfig::GetConfig(*db.instance);
  config.AddExtensionOption("zipfs_extension", "DESCRIPTION",
                            LogicalType::VARCHAR, Value(".zip"));
  config.AddExtensionOption("zipfs_extension_replacement", "DESCRIPTION",
                            LogicalType::VARCHAR, Value("/"));
}

std::string ZipfsExtension::Name() { return "zipfs"; }

std::string ZipfsExtension::Version() const {
#ifdef EXT_VERSION_ZIPFS
  return EXT_VERSION_ZIPFS;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void zipfs_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::ZipfsExtension>();
}

DUCKDB_EXTENSION_API const char *zipfs_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
