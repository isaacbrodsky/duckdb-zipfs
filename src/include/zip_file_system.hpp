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

// Streaming file handle for entries inside a zip archive (zip://). The
// decompressed bytes are NEVER fully materialized in memory: miniz's
// extract-iterator is kept open and read incrementally, so entries whose
// uncompressed size is larger than available memory can be read.
//
// Random access is supported by seeking: forward seeks inflate-and-discard,
// backward seeks restart the iterator from the entry start. Sequential forward
// reads (the common CSV/NDJSON case) are O(n) with O(ZIP_BLOCK_SIZE) memory.
class ZipFileHandle final : public FileHandle {
  friend class ZipFileSystem;

public:
  ZipFileHandle(FileSystem &file_system, const string &path,
                FileOpenFlags flags, unique_ptr<FileHandle> inner_handle_p)
      : FileHandle(file_system, path, flags),
        inner_handle(std::move(inner_handle_p)), zip_inited(false),
        file_stat({0}), file_index(0), iter(nullptr), stream_pos(0),
        seek_offset(0), stream_finished(false) {
    scratch = make_uniq_array2<data_t>(ZIP_BLOCK_SIZE);
  }

  ~ZipFileHandle() override;
  void Close() override;

  // Sequential read at the current logical position (advances it).
  int64_t ReadBytes(void *buffer, int64_t nr_bytes);
  // Positional read; does not move the logical position.
  void ReadBytesAt(void *buffer, int64_t nr_bytes, idx_t location);
  int64_t Size() const { return static_cast<int64_t>(file_stat.m_uncomp_size); }

private:
  // (Re)start the miniz extract iterator at the start of the entry.
  void InitStream();
  void CloseStream(bool throw_on_error = false);
  int64_t ReadStream(void *buffer, int64_t nr_bytes);
  void SeekTo(idx_t target);

  unique_ptr<FileHandle> inner_handle;
  mz_zip_archive zip; // central directory reader; kept open for the lifetime
  bool zip_inited;
  mz_zip_archive_file_stat file_stat;
  mz_uint file_index;
  mz_zip_reader_extract_iter_state *iter; // current streaming iterator
  idx_t stream_pos;                       // decompressed bytes consumed
  idx_t seek_offset;                      // logical cursor for sequential reads
  bool stream_finished;                   // iterator reached validated EOF
  unique_ptr<data_t[]> scratch;           // discard buffer for forward seeks
  std::mutex stream_lock;
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
