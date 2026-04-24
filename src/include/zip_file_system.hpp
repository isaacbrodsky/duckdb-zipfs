#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>

namespace duckdb {

auto const ZIP_SEPARATOR = "/";

size_t FileSystemZipReadFunc(void *pOpaque, mz_uint64 file_ofs, void *pBuf,
                             size_t n);

class ZipFileHandle final : public FileHandle {
  friend class ZipFileSystem;

public:
  ZipFileHandle(FileSystem &file_system, const string &path,
                FileOpenFlags flags, unique_ptr<FileHandle> inner_handle_p,
                const mz_zip_archive_file_stat &file_stat,
                unique_ptr<data_t[]> data)
      : FileHandle(file_system, path, flags),
        inner_handle(std::move(inner_handle_p)), file_stat(file_stat),
        data(std::move(data)), seek_offset(0) {}

  void Close() override;

private:
  unique_ptr<FileHandle> inner_handle;
  mz_zip_archive_file_stat file_stat;
  unique_ptr<data_t[]> data;
  idx_t seek_offset;
};

class ZipStreamFileHandle final : public FileHandle {
  friend class ZipStreamFileSystem;

public:
  ZipStreamFileHandle(FileSystem &file_system, const string &path,
                      FileOpenFlags flags,
                      unique_ptr<FileHandle> inner_handle_p,
                      const mz_zip_archive_file_stat &file_stat,
                      mz_uint file_index)
      : FileHandle(file_system, path, flags),
        inner_handle(std::move(inner_handle_p)), file_stat(file_stat),
        file_index(file_index), iter_state(nullptr), seek_offset(0),
        closed(false) {
    mz_zip_zero_struct(&zip);
  }

  ~ZipStreamFileHandle() override;
  void Close() override;

private:
  unique_ptr<FileHandle> inner_handle;
  mz_zip_archive zip;
  mz_zip_archive_file_stat file_stat;
  mz_uint file_index;
  mz_zip_reader_extract_iter_state *iter_state;
  idx_t seek_offset;
  bool closed;
};

class ZipFileSystem final : public FileSystem {
public:
  explicit ZipFileSystem() : FileSystem() {}

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
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
};

class ZipStreamFileSystem final : public FileSystem {
public:
  explicit ZipStreamFileSystem() : FileSystem() {}

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  void Reset(FileHandle &handle) override;
  idx_t SeekPosition(FileHandle &handle) override;
  std::string GetName() const override { return "ZipStreamFileSystem"; }
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
