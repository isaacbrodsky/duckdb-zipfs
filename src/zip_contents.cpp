#include "zip_contents.hpp"
#include "zip_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct ReadZipFunctionData : public GlobalTableFunctionState {
  ReadZipFunctionData() : finished(false) {}
  bool finished;
};

struct ReadZipFunctionBindData : public TableFunctionData {
  string file_path;
};

void ReadZipFunction(ClientContext &context, TableFunctionInput &data,
                     DataChunk &output) {
  auto bind_data = data.bind_data->Cast<ReadZipFunctionBindData>();
  auto &global_data = data.global_state->Cast<ReadZipFunctionData>();
  if (global_data.finished) {
    return;
  }
  auto &zip_path = bind_data.file_path;

  auto &fs = FileSystem::GetFileSystem(context);
  if (!fs.FileExists(zip_path)) {
    throw IOException("Zip file does not exist: %s", zip_path);
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

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();

  string zip_filename;
  const size_t MAX_FILENAME_LEN = 65536; // = 2**16
  zip_filename.reserve(1024);
  try {
    mz_uint flags = 0;

    if (!mz_zip_reader_init(&zip, size, flags)) {
      throw IOException("Could not open as zip file: %s",
                        mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }

    mz_uint i, files;

    files = mz_zip_reader_get_num_files(&zip);

    for (i = 0; i < files; i++) {
      mz_zip_clear_last_error(&zip);

      mz_zip_validate_file(&zip, i, MZ_ZIP_FLAG_VALIDATE_HEADERS_ONLY);

      mz_zip_clear_last_error(&zip);

      mz_uint filename_size = mz_zip_reader_get_filename(&zip, i, nullptr, 0);
      // NOTE: filename_size already contains +1 for the leading \0
      // Double filename capacity/length until it's enough or larger than
      // 2**16, where 2**16 should be the max filename length in zip files.
      if (filename_size > zip_filename.capacity()) {
        size_t new_capacity =
            zip_filename.capacity() > 0 ? zip_filename.capacity() : 1;
        while (new_capacity < filename_size) {
          new_capacity *= 2;
          if (new_capacity > MAX_FILENAME_LEN) {
            throw IOException("Filename too long");
          }
        }
        zip_filename.reserve(new_capacity);
      }
      zip_filename.resize(filename_size - 1);
      mz_zip_reader_get_filename(&zip, i, &zip_filename[0], filename_size);

      if (auto err = mz_zip_get_last_error(&zip)) {
        throw IOException("Problem getting filename: %s",
                          mz_zip_get_error_string(err));
      }

      mz_zip_archive_file_stat stat;
      mz_zip_reader_file_stat(&zip, i, &stat);

      if (auto err = mz_zip_get_last_error(&zip)) {
        throw IOException("Problem statting file: %s",
                          mz_zip_get_error_string(err));
      }

      idx_t col = 0;
      output.SetValue(col++, count, Value(zip_filename));
      output.SetValue(
          col++, count,
          Value::UBIGINT(NumericCast<uint64_t>(stat.m_uncomp_size)));
      output.SetValue(col++, count, Value::BOOLEAN(stat.m_is_directory));

      count++;
    }

    mz_zip_reader_end(&zip);
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }

  output.SetCardinality(count);
  global_data.finished = true;
}

unique_ptr<FunctionData> ReadZipFunctionBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names) {
  auto result = make_uniq<ReadZipFunctionBindData>();
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
ReadZipFunctionInit(ClientContext &context, TableFunctionInitInput &input) {
  return std::move(make_uniq<ReadZipFunctionData>());
}

} // namespace duckdb
