#include "zipfs_extension.hpp"
#include "zip_file_system.hpp"
#include "archive_file_system.hpp"
#include "noop_archive_file_system.hpp"
#include "zip_contents.hpp"
#include "archive_contents.hpp"
#include "noop_archive_contents.hpp"
#include "duckdb.hpp"
#include "zipfs_secret.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
  std::string description = "Support for reading files from zip archives";
  loader.SetDescription(description);

  auto &fs = loader.GetDatabaseInstance().GetFileSystem();
  fs.RegisterSubSystem(make_uniq<ZipFileSystem>());
  auto zipContents =
      TableFunction("zip_contents", {LogicalType::VARCHAR}, ReadZipFunction,
                    ReadZipFunctionBind, ReadZipFunctionInit);

  auto createZipContents = CreateTableFunctionInfo(zipContents);
  FunctionDescription zipDesc;
  zipDesc.parameter_names = {"zip_path"};
  zipDesc.parameter_types = {LogicalType::VARCHAR};
  zipDesc.description = "Returns a table of the files in the zip archive.";
  zipDesc.examples = {"SELECT * FROM zip_contents('example.zip');",
                      "SELECT file_name, file_size, is_directory, is_encrypted "
                      "FROM zip_contents('example.zip');"};
  zipDesc.categories = {"zip"};
  createZipContents.descriptions.push_back(zipDesc);
  loader.RegisterFunction(createZipContents);

#ifdef ENABLE_LIBARCHIVE
  fs.RegisterSubSystem(make_uniq<ArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<RawArchiveFileSystem>());
  auto archiveContents = TableFunction(
      "archive_contents", {LogicalType::VARCHAR}, ReadArchiveFunction,
      ReadArchiveFunctionBind, ReadArchiveFunctionInit);
  auto createArchiveContents = CreateTableFunctionInfo(archiveContents);
  FunctionDescription archiveDesc;
  archiveDesc.parameter_names = {"archive_path"};
  archiveDesc.parameter_types = {LogicalType::VARCHAR};
  archiveDesc.description = "Returns a table of the files in the archive.";
  archiveDesc.examples = {"SELECT * FROM archive_contents('example.zip');",
                          "SELECT file_name, file_size, is_directory, "
                          "is_encrypted FROM archive_contents('example.zip');"};
  archiveDesc.categories = {"zip"};
  createArchiveContents.descriptions.push_back(archiveDesc);
  loader.RegisterFunction(createArchiveContents);
#else
  fs.RegisterSubSystem(make_uniq<NoopArchiveFileSystem>());
  fs.RegisterSubSystem(make_uniq<NoopRawArchiveFileSystem>());
  auto archiveContents = TableFunction(
      "archive_contents", {LogicalType::VARCHAR}, NoopReadArchiveFunction,
      NoopReadArchiveFunctionBind, NoopReadArchiveFunctionInit);
  auto createArchiveContents = CreateTableFunctionInfo(archiveContents);
  FunctionDescription archiveDesc;
  archiveDesc.parameter_names = {"archive_path"};
  archiveDesc.parameter_types = {LogicalType::VARCHAR};
  archiveDesc.description = "Returns a table of the files in the archive.";
  archiveDesc.examples = {"SELECT * FROM archive_contents('example.zip');",
                          "SELECT file_name, file_size, is_directory, "
                          "is_encrypted FROM archive_contents('example.zip');"};
  archiveDesc.categories = {"zip"};
  createArchiveContents.descriptions.push_back(archiveDesc);
  loader.RegisterFunction(createArchiveContents);
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

  SecretType zipfsSecretType;
  zipfsSecretType.name = "zip";
  zipfsSecretType.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
  zipfsSecretType.default_provider = "config";
  zipfsSecretType.extension = "zipfs";
  loader.RegisterSecretType(zipfsSecretType);

  CreateSecretFunction createZipfsSecret = {"zip", "config",
                                            CreateZipSecretFunction};
  createZipfsSecret.named_parameters["password"] = LogicalType::VARCHAR;
  createZipfsSecret.named_parameters["passwords"] =
      LogicalType::LIST(LogicalType::VARCHAR);
  loader.RegisterFunction(createZipfsSecret);
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
