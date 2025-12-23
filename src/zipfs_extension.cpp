#include "zipfs_extension.hpp"
#include "zip_file_system.hpp"
#include "archive_file_system.hpp"
#include "noop_archive_file_system.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
  std::string description = "Support for reading files from zip archives";
  loader.SetDescription(description);

  auto &fs = loader.GetDatabaseInstance().GetFileSystem();
  fs.RegisterSubSystem(make_uniq<ZipFileSystem>());
#ifdef ENABLE_LIBARCHIVE
  fs.RegisterSubSystem(make_uniq<ArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<RawArchiveFileSystem>());
#else
  fs.RegisterSubSystem(make_uniq<NoopArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<NoopRawArchiveFileSystem>());
#endif // ENABLE_LIBARCHIVE

  auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
  config.AddExtensionOption(
      "zipfs_extension",
      "Extension to look for splitting the zip path and "
      "the file path within the zip. To specify an artificial seperator, "
      "instead set: `set zipfs_split = '!!';`",
      LogicalType::VARCHAR, Value(".zip"));
  config.AddExtensionOption(
      "zipfs_split",
      "Extension to look for splitting the zip path and "
      "the file path within the zip. Will be removed from the zip file name. "
      "Overrides zipfs_extension. Defaults to NULL.",
      LogicalType::VARCHAR, Value(LogicalType::VARCHAR));
}

void ZipfsExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

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

DUCKDB_CPP_EXTENSION_ENTRY(zipfs, loader) { duckdb::LoadInternal(loader); }
}
