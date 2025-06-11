#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <archive.h>
#include <archive_entry.h>

namespace duckdb {

const size_t BLOCK_SIZE = 1024 * 10;

// TODO: Something is incorrect about the type in make_uniq_array<...,
// std::default_delete<DATA_TYPE>, ...>
template <class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>
make_uniq_array2(size_t n) // NOLINT: mimic std style
{
  return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>(
      new DATA_TYPE[n]());
}

class ZipArchiveHandle final {
public:
  ZipArchiveHandle(unique_ptr<FileHandle> inner_handle_p)
      : inner_handle(std::move(inner_handle_p)) {
    data = make_uniq_array2<data_t>(BLOCK_SIZE);
    data_len = BLOCK_SIZE;
  }

  unique_ptr<FileHandle> inner_handle;
  unique_ptr<data_t[]> data;
  size_t data_len;
};

class ZipFileHandle final : public FileHandle {
  friend class ZipFileSystem;

public:
  ZipFileHandle(FileSystem &file_system, const string &path,
                FileOpenFlags flags, time_t &last_modified_time,
                bool has_last_modified_time, FileType file_type,
                bool on_disk_file, size_t sz, unique_ptr<data_t[]> data)
      : FileHandle(file_system, path, flags),
        last_modified_time(last_modified_time),
        has_last_modified_time(has_last_modified_time), file_type(file_type),
        on_disk_file(on_disk_file), sz(sz), data(std::move(data)),
        seek_offset(0) {}

  void Close() override;

private:
  time_t last_modified_time;
  bool has_last_modified_time;
  FileType file_type;
  bool on_disk_file;

  size_t sz;
  unique_ptr<data_t[]> data;
  idx_t seek_offset;
};

class ZipFileSystem final : public FileSystem {
public:
  explicit ZipFileSystem() : FileSystem() { // parent_file_system(parent_p) {
  }

  time_t GetLastModifiedTime(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  void Reset(FileHandle &handle) override;
  idx_t SeekPosition(FileHandle &handle) override;
  std::string GetName() const override { return "ZipFileSystem"; }
  vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override;
  bool FileExists(const string &filename,
                  optional_ptr<FileOpener> opener) override;

  bool CanHandleFile(const string &fpath) override;
  bool OnDiskFile(FileHandle &handle) override;
  bool CanSeek() override;

  unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                  optional_ptr<FileOpener> opener) override;

private:
  // FileSystem &parent_file_system;
};

} // namespace duckdb