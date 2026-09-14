#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>
#include <mutex>
#include "utils.hpp"

namespace duckdb {

auto const ZIP_SEPARATOR = "/";
const size_t ZIP_BLOCK_SIZE = 1024 * 10;

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

// A compressed (DEFLATE) member streamed on demand via miniz's extract
// iterator. The decompressed bytes are NEVER fully materialized: the iterator
// is kept open and read incrementally, so entries larger than memory can be
// read. Compressed members are NOT randomly seekable (CanSeek() == false), so
// DuckDB reads them forward; ReadInto only supports non-decreasing locations.
class StreamingZipFileHandle final : public ZipFileHandle {
public:
  StreamingZipFileHandle(FileSystem &file_system, const string &path,
                         FileOpenFlags flags,
                         unique_ptr<FileHandle> inner_handle_p,
                         const mz_zip_archive_file_stat &file_stat,
                         mz_zip_archive *zip, mz_uint file_index)
      : ZipFileHandle(file_system, path, flags, std::move(inner_handle_p),
                      file_stat),
        zip(zip), file_index(file_index), iter(nullptr), stream_pos(0) {
    scratch = make_uniq_array2<data_t>(ZIP_BLOCK_SIZE);
  }

  ~StreamingZipFileHandle() override;
  void Close() override;

  // Compressed members cannot be randomly seeked.
  bool CanSeek() override { return false; }

  void ReadInto(void *buffer, idx_t nr_bytes, idx_t location) override;

private:
  void InitStream();
  void CloseStream();

  mz_zip_archive *zip;  // central-directory reader, owned by the handle
  mz_uint file_index;
  mz_zip_reader_extract_iter_state *iter; // current streaming iterator
  idx_t stream_pos;                       // decompressed bytes consumed
  unique_ptr<data_t[]> scratch;           // discard buffer for forward skips
  std::mutex stream_lock;
};

// A stored (uncompressed) member forwarding reads to the underlying handle at
// data_offset. Fully seekable random access.
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

  // Stored members map directly onto the underlying file: true random access.
  bool CanSeek() override { return true; }

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
