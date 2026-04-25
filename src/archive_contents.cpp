#include "archive_contents.hpp"
#include "archive_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

#ifdef ENABLE_LIBARCHIVE

namespace duckdb {

struct ReadArchiveFunctionData : public GlobalTableFunctionState {
  ReadArchiveFunctionData() : finished(false) {}
  bool finished;
};

struct ReadArchiveFunctionBindData : public TableFunctionData {
  string file_path;
};

void ReadArchiveFunction(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output) {
  auto bind_data = data.bind_data->Cast<ReadArchiveFunctionBindData>();
  auto &global_data = data.global_state->Cast<ReadArchiveFunctionData>();
  if (global_data.finished) {
    return;
  }
  auto &zip_path = bind_data.file_path;

  auto &fs = FileSystem::GetFileSystem(context);
  if (!fs.FileExists(zip_path)) {
    throw IOException("Archive file does not exist: %s", zip_path);
  }

  auto handle = fs.OpenFile(zip_path, FileOpenFlags::FILE_FLAGS_READ);
  if (!handle) {
    throw IOException("Failed to open file: %s", zip_path);
  }

  if (!handle->CanSeek()) {
    throw IOException("Cannot seek");
  }

  idx_t size = handle->GetFileSize();
  idx_t count = 0;

  struct archive *archive = archive_read_new();
  try {
    if (archive_read_support_filter_all(archive)) {
      throw IOException("Failed to init libarchive (filter all): %s",
                        archive_error_string(archive));
    }
    if (archive_read_support_format_all(archive)) {
      throw IOException("Failed to init libarchive (format all): %s",
                        archive_error_string(archive));
    }
    unique_ptr<LibArchiveHandle> zipHandle =
        make_uniq<LibArchiveHandle>(std::move(handle));
    // TODO: Add skip?
    if (archive_read_set_seek_callback(archive, FileSystemZipSeekFunc)) {
      throw IOException("Failed to init libarchive (seek callback): %s",
                        archive_error_string(archive));
    }
    if (archive_read_open(archive, zipHandle.get(), &FileSystemZipOpenFunc,
                          &FileSystemZipReadFunc, &FileSystemZipCloseFunc)) {
      throw IOException("Failed to init libarchive (read callback): %s",
                        archive_error_string(archive));
    }
    struct archive_entry *entry = archive_entry_new2(archive);
    try {
      while (archive_read_next_header2(archive, entry) == ARCHIVE_OK) {
        auto pathName = archive_entry_pathname(entry);
        auto fileSize = archive_entry_size(entry);
        auto fileType = archive_entry_filetype(entry);
        auto isDir = fileType == AE_IFDIR;

        idx_t col = 0;
        output.SetValue(col++, count, Value(pathName));
        output.SetValue(col++, count,
                        Value::UBIGINT(NumericCast<uint64_t>(fileSize)));
        output.SetValue(col++, count, Value::BOOLEAN(isDir));

        count++;
      }

      archive_entry_free(entry);
      archive_read_free(archive);
    } catch (Exception &ex2) {
      archive_entry_free(entry);
      throw;
    }
  } catch (IOException &ex) {
    archive_read_free(archive);
    throw;
  } catch (Exception &ex) {
    archive_read_free(archive);
    throw;
  }

  output.SetCardinality(count);
  global_data.finished = true;
}

unique_ptr<FunctionData>
ReadArchiveFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<string> &names) {
  auto result = make_uniq<ReadArchiveFunctionBindData>();
  result->file_path = input.inputs[0].GetValue<string>();

  return_types.push_back(LogicalType::VARCHAR);
  names.emplace_back("file_name");

  return_types.push_back(LogicalType::UBIGINT);
  names.emplace_back("file_size");

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("is_directory");

  return result;
}

unique_ptr<GlobalTableFunctionState>
ReadArchiveFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
  return std::move(make_uniq<ReadArchiveFunctionData>());
}

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
