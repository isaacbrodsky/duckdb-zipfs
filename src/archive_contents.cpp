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
  ReadArchiveFunctionData(ClientContext &context, const string &archive_path)
      : archive(nullptr), entry(nullptr), finished(false) {
    try {
      auto &fs = FileSystem::GetFileSystem(context);
      if (!fs.FileExists(archive_path)) {
        throw IOException("Archive file does not exist: %s", archive_path);
      }

      auto handle = fs.OpenFile(archive_path, FileOpenFlags::FILE_FLAGS_READ);
      if (!handle) {
        throw IOException("Failed to open file: %s", archive_path);
      }

      if (!handle->CanSeek()) {
        throw IOException("Cannot seek");
      }

      file_handle = make_uniq<LibArchiveHandle>(std::move(handle));

      archive = archive_read_new();
      if (!archive) {
        throw IOException("Failed to init libarchive (read new): %s",
                          archive_error_string(archive));
      }
      if (archive_read_support_filter_all(archive)) {
        throw IOException("Failed to init libarchive (filter all): %s",
                          archive_error_string(archive));
      }
      if (archive_read_support_format_all(archive)) {
        throw IOException("Failed to init libarchive (format all): %s",
                          archive_error_string(archive));
      }
      // TODO: Add skip?
      if (archive_read_set_seek_callback(archive, FileSystemZipSeekFunc)) {
        throw IOException("Failed to init libarchive (seek callback): %s",
                          archive_error_string(archive));
      }
      if (archive_read_open(archive, file_handle.get(), &FileSystemZipOpenFunc,
                            &FileSystemZipReadFunc, &FileSystemZipCloseFunc)) {
        throw IOException("Failed to init libarchive (read callback): %s",
                          archive_error_string(archive));
      }
      entry = archive_entry_new2(archive);
      if (!entry) {
        throw IOException("Failed to allocate archive entry");
      }
    } catch (...) {
      Close();
      throw;
    }
  }

  ReadArchiveFunctionData(const ReadArchiveFunctionData &) = delete;
  ReadArchiveFunctionData &operator=(const ReadArchiveFunctionData &) = delete;

  ~ReadArchiveFunctionData() override { Close(); }

  void Close() {
    if (entry) {
      archive_entry_free(entry);
      entry = nullptr;
    }
    if (archive) {
      archive_read_free(archive);
      archive = nullptr;
    }
  }

  unique_ptr<LibArchiveHandle> file_handle;
  struct archive *archive;
  struct archive_entry *entry;
  bool finished;
};

struct ReadArchiveFunctionBindData : public TableFunctionData {
  string file_path;
};

void ReadArchiveFunction(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output) {
  auto &global_data = data.global_state->Cast<ReadArchiveFunctionData>();

  idx_t count = 0;
  idx_t capacity = ChunkSize(output);
  while (!global_data.finished && count < capacity) {
    if (archive_read_next_header2(global_data.archive, global_data.entry) !=
        ARCHIVE_OK) {
      global_data.finished = true;
      global_data.Close();
      break;
    }

    auto entry = global_data.entry;
    auto pathName = archive_entry_pathname(entry);
    auto fileSize = archive_entry_size(entry);
    auto fileType = archive_entry_filetype(entry);
    auto isEncrypted = archive_entry_is_encrypted(entry);
    auto isDir = fileType == AE_IFDIR;

    idx_t col = 0;
    output.data[col++].Append(pathName);
    output.data[col++].Append(Value::UBIGINT(NumericCast<uint64_t>(fileSize)));
    output.data[col++].Append(Value::BOOLEAN(isDir));
    output.data[col++].Append(Value::BOOLEAN(isEncrypted));

    count++;
  }

  output.CheckCardinality(count);
}

unique_ptr<FunctionData>
ReadArchiveFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<Identifier> &names) {
  auto result = make_uniq<ReadArchiveFunctionBindData>();
  result->file_path = input.inputs[0].GetValue<string>();

  return_types.push_back(LogicalType::VARCHAR);
  names.emplace_back("file_name");

  return_types.push_back(LogicalType::UBIGINT);
  names.emplace_back("file_size");

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("is_directory");

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("is_encrypted");

  return result;
}

unique_ptr<GlobalTableFunctionState>
ReadArchiveFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<ReadArchiveFunctionBindData>();
  return make_uniq<ReadArchiveFunctionData>(context, bind_data.file_path);
}

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
