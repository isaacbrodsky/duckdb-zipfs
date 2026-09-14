#pragma once

#ifdef ENABLE_LIBARCHIVE

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include <archive.h>
#include <archive_entry.h>
#include <atomic>
#include <mutex>
#include "utils.hpp"

namespace duckdb {

la_ssize_t FileSystemZipReadFunc(struct archive *archive, void *clientData,
                                 const void **buffer);

la_int64_t FileSystemZipSeekFunc(struct archive *archive, void *clientData,
                                 la_int64_t offset, int whence);

int FileSystemZipOpenFunc(struct archive *archive, void *clientData);

int FileSystemZipCloseFunc(struct archive *archive, void *clientData);

const size_t BLOCK_SIZE = 1024 * 10;

class LibArchiveHandle final {
public:
  LibArchiveHandle(unique_ptr<FileHandle> inner_handle_p)
      : inner_handle(std::move(inner_handle_p)) {
    data = make_uniq_array2<data_t>(BLOCK_SIZE);
    data_len = BLOCK_SIZE;
  }

  unique_ptr<FileHandle> inner_handle;
  unique_ptr<data_t[]> data;
  size_t data_len;
};

// Streaming file handle for libarchive backed entries (archive:// and
// compressed://). The decompressed bytes are NEVER fully materialized in
// memory: the libarchive decompression stream is kept open and read
// incrementally. This allows reading entries whose uncompressed size is larger
// than available memory.
//
// Random access is supported by seeking: forward seeks decompress-and-discard,
// backward seeks re-open the stream from the start. Sequential forward reads
// (the common CSV/NDJSON case) are O(n) with O(BLOCK_SIZE) memory.
class ArchiveFileHandle final : public FileHandle {
  friend class ArchiveFileSystem;
  friend class RawArchiveFileSystem;

public:
  ArchiveFileHandle(FileSystem &file_system, const string &path,
                    FileOpenFlags flags, timestamp_t last_modified_time,
                    bool has_last_modified_time, FileType file_type,
                    bool on_disk_file, unique_ptr<LibArchiveHandle> lib_handle,
                    bool raw_format, string entry_name)
      : FileHandle(file_system, path, flags),
        last_modified_time(last_modified_time),
        has_last_modified_time(has_last_modified_time), file_type(file_type),
        on_disk_file(on_disk_file), lib_handle(std::move(lib_handle)),
        raw_format(raw_format), entry_name(std::move(entry_name)),
        archive(nullptr), stream_pos(0), seek_offset(0), sz(-1) {
    scratch = make_uniq_array2<data_t>(BLOCK_SIZE);
  }

  ~ArchiveFileHandle() override;
  void Close() override;

  // Sequential read at the current logical position (advances it).
  int64_t ReadBytes(void *buffer, int64_t nr_bytes);
  // Positional read; does not move the logical position.
  void ReadBytesAt(void *buffer, int64_t nr_bytes, idx_t location);
  // Uncompressed size. Computed lazily (streaming discard pass) when the
  // archive entry does not record it (e.g. raw gzip/bz2).
  int64_t Size();

private:
  // (Re)open the libarchive decompression stream positioned at the start of the
  // target entry. Throws if the entry cannot be found.
  void InitStream();
  void CloseStream();
  // Read up to nr_bytes of decompressed data from the current stream.
  int64_t ReadStream(void *buffer, int64_t nr_bytes);
  // Ensure the decompression stream is positioned exactly at target.
  void SeekTo(idx_t target);

  timestamp_t last_modified_time;
  bool has_last_modified_time;
  FileType file_type;
  bool on_disk_file;

  // lib_handle owns the inner file handle and the compressed-input block buffer;
  // it must outlive `archive` (which references it via the read callbacks).
  unique_ptr<LibArchiveHandle> lib_handle;
  bool raw_format;   // true => archive_read_support_format_raw (compressed://)
  string entry_name; // entry to extract; ignored when raw_format

  struct archive *archive; // current open decompression stream (or nullptr)
  idx_t stream_pos;        // decompressed bytes consumed from current stream
  // logical cursor for sequential reads; atomic because the FileSystem
  // Seek/Reset/SeekPosition entry points touch it without holding stream_lock.
  std::atomic<idx_t> seek_offset;
  int64_t sz;              // uncompressed size, or -1 if not yet known
  unique_ptr<data_t[]> scratch; // discard buffer for forward seeks / size probe
  std::mutex stream_lock;
};

class ArchiveFileSystem final : public FileSystem {
public:
  explicit ArchiveFileSystem() : FileSystem() {}

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  void Reset(FileHandle &handle) override;
  idx_t SeekPosition(FileHandle &handle) override;
  std::string GetName() const override { return "ArchiveFileSystem"; }

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

class RawArchiveFileSystem final : public FileSystem {
public:
  explicit RawArchiveFileSystem() : FileSystem() {}

  timestamp_t GetLastModifiedTime(FileHandle &handle) override;
  FileType GetFileType(FileHandle &handle) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  int64_t GetFileSize(FileHandle &handle) override;
  void Seek(FileHandle &handle, idx_t location) override;
  void Reset(FileHandle &handle) override;
  idx_t SeekPosition(FileHandle &handle) override;
  std::string GetName() const override { return "RawArchiveFileSystem"; }

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

#endif // ENABLE_LIBARCHIVE
