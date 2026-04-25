#include "zipfs_extension.hpp"
#include "archive_contents.hpp"
#include "archive_file_system.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "noop_archive_contents.hpp"
#include "noop_archive_file_system.hpp"
#include "zip_contents.hpp"
#include "zip_file_system.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
  std::string description = "Support for reading files from zip archives";
  loader.SetDescription(description);

  auto &fs = loader.GetDatabaseInstance().GetFileSystem();
  fs.RegisterSubSystem(make_uniq<ZipFileSystem>());
  fs.RegisterSubSystem(make_uniq<ZipStreamFileSystem>());
  loader.RegisterFunction(TableFunction("zip_contents", {LogicalType::VARCHAR},
                                        ReadZipFunction, ReadZipFunctionBind,
                                        ReadZipFunctionInit));

#ifdef ENABLE_LIBARCHIVE
  fs.RegisterSubSystem(make_uniq<ArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<RawArchiveFileSystem>());
  loader.RegisterFunction(TableFunction(
      "archive_contents", {LogicalType::VARCHAR}, ReadArchiveFunction,
      ReadArchiveFunctionBind, ReadArchiveFunctionInit));
#else
  fs.RegisterSubSystem(make_uniq<NoopArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<NoopRawArchiveFileSystem>());
  loader.RegisterFunction(TableFunction(
      "archive_contents", {LogicalType::VARCHAR}, NoopReadArchiveFunction,
      NoopReadArchiveFunctionBind, NoopReadArchiveFunctionInit));
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
  config.AddExtensionOption(
      "zipfs_seek_threshold",
      "Members larger than this many uncompressed bytes use zran-based random "
      "access instead of full buffering.",
      LogicalType::BIGINT, Value::BIGINT(268435456));
  config.AddExtensionOption(
      "zipfs_zran_span",
      "Spacing between zran access points in uncompressed bytes.",
      LogicalType::BIGINT, Value::BIGINT(1048576));
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
