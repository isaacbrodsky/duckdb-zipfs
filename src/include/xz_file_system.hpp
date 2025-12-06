#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <lzma.h>

namespace duckdb {

class XzFileHandle final : public FileHandle {
  friend class XzFileSystem;

public:
  XzFileHandle(FileSystem &file_system, const string &path, FileOpenFlags flags,
               unique_ptr<FileHandle> inner_handle_p,
               unique_ptr<data_t[]> data_p, idx_t data_size_p)
      : FileHandle(file_system, path, flags),
        inner_handle(std::move(inner_handle_p)), data(std::move(data_p)),
        data_size(data_size_p), seek_offset(0) {}

  void Close() override;

private:
  unique_ptr<FileHandle> inner_handle;

  // Decompressed data buffer (entire file)
  unique_ptr<data_t[]> data;
  idx_t data_size;

  // Current seek position
  idx_t seek_offset;
};

class XzFileSystem final : public FileSystem {
public:
  explicit XzFileSystem() : FileSystem() {}

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  void Reset(FileHandle &handle) override;
  idx_t SeekPosition(FileHandle &handle) override;
  std::string GetName() const override { return "XzFileSystem"; }
  vector<OpenFileInfo> Glob(const string &path, FileOpener *opener) override;
  bool FileExists(const string &filename,
                  optional_ptr<FileOpener> opener) override;

  bool CanHandleFile(const string &fpath) override;
  bool OnDiskFile(FileHandle &handle) override;
  bool CanSeek() override;

  unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                  optional_ptr<FileOpener> opener) override;

private:
};

} // namespace duckdb
