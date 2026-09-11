#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>

namespace duckdb {

auto const ZIP_SEPARATOR = "/";

size_t FileSystemZipReadFunc(void *pOpaque, mz_uint64 file_ofs, void *pBuf,
                             size_t n);

class ZipFileHandle : public FileHandle {
  friend class ZipFileSystem;

public:
  ZipFileHandle(FileSystem &file_system, const string &path,
                FileOpenFlags flags, unique_ptr<FileHandle> inner_handle_p,
                const mz_zip_archive_file_stat &file_stat)
      : FileHandle(file_system, path, flags),
        inner_handle(std::move(inner_handle_p)), file_stat(file_stat),
        seek_offset(0) {}

  virtual void ReadInto(void *buffer, idx_t nr_bytes, idx_t location) = 0;

  void Close() override;

protected:
  unique_ptr<FileHandle> inner_handle;
  mz_zip_archive_file_stat file_stat;
  idx_t seek_offset;
};

// A compressed (DEFLATE) member decompressed up front into an owned buffer.
class BufferedZipFileHandle final : public ZipFileHandle {
public:
  BufferedZipFileHandle(FileSystem &file_system, const string &path,
                        FileOpenFlags flags,
                        unique_ptr<FileHandle> inner_handle_p,
                        const mz_zip_archive_file_stat &file_stat,
                        unique_ptr<data_t[]> data)
      : ZipFileHandle(file_system, path, flags, std::move(inner_handle_p),
                      file_stat),
        data(std::move(data)) {}

  void ReadInto(void *buffer, idx_t nr_bytes, idx_t location) override {
    memcpy(buffer, data.get() + location, nr_bytes);
  }

private:
  unique_ptr<data_t[]> data;
};

// A stored (uncompressed) member forwarding reads to the underlying handle at data_offset.
class WindowedZipFileHandle final : public ZipFileHandle {
public:
  WindowedZipFileHandle(FileSystem &file_system, const string &path,
                        FileOpenFlags flags,
                        unique_ptr<FileHandle> inner_handle_p,
                        const mz_zip_archive_file_stat &file_stat,
                        idx_t data_offset)
      : ZipFileHandle(file_system, path, flags, std::move(inner_handle_p),
                      file_stat),
        data_offset(data_offset) {}

  void ReadInto(void *buffer, idx_t nr_bytes, idx_t location) override {
    inner_handle->Read(buffer, nr_bytes, data_offset + location);
  }

private:
  idx_t data_offset;
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

} // namespace duckdb
