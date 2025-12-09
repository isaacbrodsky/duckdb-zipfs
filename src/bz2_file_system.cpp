#include "bz2_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstring>
#include <vector>

namespace duckdb {

// Helper to allocate unique_ptr arrays (same pattern as zip_file_system.cpp)
template <class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>
make_uniq_array_bz2(size_t n) {
  return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>(
      new DATA_TYPE[n]());
}

//------------------------------------------------------------------------------
// Bz2 File Handle
//------------------------------------------------------------------------------

void Bz2FileHandle::Close() { inner_handle->Close(); }

//------------------------------------------------------------------------------
// Bz2 File System
//------------------------------------------------------------------------------

static const string BZ2_PREFIX = "bz2://";
static const string BZIP2_PREFIX = "bzip2://";
static const string BZ2_SUFFIX = ".bz2";

// Check if path has bz2:// or bzip2:// prefix
static bool HasBz2Prefix(const string &path) {
  return (path.size() > BZ2_PREFIX.size() &&
          path.substr(0, BZ2_PREFIX.size()) == BZ2_PREFIX) ||
         (path.size() > BZIP2_PREFIX.size() &&
          path.substr(0, BZIP2_PREFIX.size()) == BZIP2_PREFIX);
}

// Check if path ends with .bz2
static bool HasBz2Suffix(const string &path) {
  if (path.size() < BZ2_SUFFIX.size()) {
    return false;
  }
  auto lower = StringUtil::Lower(path);
  return lower.substr(lower.size() - BZ2_SUFFIX.size()) == BZ2_SUFFIX;
}

// Returns the actual file path (strips prefix if present, keeps as-is for
// suffix)
static string GetBz2FilePath(const string &path) {
  if (path.size() > BZ2_PREFIX.size() &&
      path.substr(0, BZ2_PREFIX.size()) == BZ2_PREFIX) {
    return path.substr(BZ2_PREFIX.size());
  }
  if (path.size() > BZIP2_PREFIX.size() &&
      path.substr(0, BZIP2_PREFIX.size()) == BZIP2_PREFIX) {
    return path.substr(BZIP2_PREFIX.size());
  }
  // No prefix - return as-is (for .bz2 suffix case)
  return path;
}

bool Bz2FileSystem::CanHandleFile(const string &fpath) {
  // Only handle files with bz2:// or bzip2:// prefix for now
  return HasBz2Prefix(fpath);
}

unique_ptr<FileHandle>
Bz2FileSystem::OpenFile(const string &path, FileOpenFlags flags,
                        optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Bz2 file system can only open for reading");
  }

  // Get the path to the bz2 file (remove prefix if present)
  if (!opener) {
    throw IOException("Bz2 file system requires a file opener");
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    throw IOException("Bz2 file system requires a client context");
  }
  auto bz2_path = GetBz2FilePath(path);

  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(bz2_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", bz2_path);
  }

  // Read entire compressed file (may require multiple reads)
  idx_t compressed_size = handle->GetFileSize();
  auto compressed_data = make_uniq_array_bz2<data_t>(compressed_size);
  idx_t total_read = 0;
  while (total_read < compressed_size) {
    auto bytes_read = handle->Read(compressed_data.get() + total_read,
                                   compressed_size - total_read);
    if (bytes_read == 0) {
      break;
    }
    total_read += bytes_read;
  }
  handle->Reset();

  // Decompress using bzip2
  // Start with 4x compressed size as initial estimate
  std::vector<data_t> decompressed;
  decompressed.reserve(compressed_size * 4);

  bz_stream strm;
  memset(&strm, 0, sizeof(strm));

  int ret = BZ2_bzDecompressInit(&strm, 0, 0);
  if (ret != BZ_OK) {
    throw IOException("Failed to initialize bzip2 decompression: error %d",
                      ret);
  }

  strm.next_in = reinterpret_cast<char *>(compressed_data.get());
  strm.avail_in = UnsafeNumericCast<unsigned int>(total_read);

  // Use larger chunks for better throughput (1MB)
  constexpr size_t CHUNK_SIZE = 1024 * 1024;
  std::vector<char> out_chunk(CHUNK_SIZE);

  while (true) {
    strm.next_out = out_chunk.data();
    strm.avail_out = CHUNK_SIZE;

    ret = BZ2_bzDecompress(&strm);

    if (ret != BZ_OK && ret != BZ_STREAM_END) {
      BZ2_bzDecompressEnd(&strm);
      throw IOException("Bzip2 decompression error: %d", ret);
    }

    size_t have = CHUNK_SIZE - strm.avail_out;
    decompressed.insert(decompressed.end(), out_chunk.begin(),
                        out_chunk.begin() + have);

    if (ret == BZ_STREAM_END) {
      // Check for concatenated bz2 streams
      if (strm.avail_in > 0) {
        // More data - reinitialize for next stream
        char *remaining_in = strm.next_in;
        unsigned int remaining_avail = strm.avail_in;
        BZ2_bzDecompressEnd(&strm);
        memset(&strm, 0, sizeof(strm));
        ret = BZ2_bzDecompressInit(&strm, 0, 0);
        if (ret != BZ_OK) {
          throw IOException("Failed to reinit bzip2: %d", ret);
        }
        strm.next_in = remaining_in;
        strm.avail_in = remaining_avail;
        continue;
      }
      break;
    }
  }

  BZ2_bzDecompressEnd(&strm);

  // Copy to unique_ptr array
  idx_t data_size = decompressed.size();
  auto data = make_uniq_array_bz2<data_t>(data_size);
  memcpy(data.get(), decompressed.data(), data_size);

  return make_uniq<Bz2FileHandle>(*this, path, flags, std::move(handle),
                                  std::move(data), data_size);
}

void Bz2FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                         idx_t location) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  auto remaining = bz_handle.data_size - location;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining);
  memcpy(buffer, bz_handle.data.get() + location, to_read);
}

int64_t Bz2FileSystem::Read(FileHandle &handle, void *buffer,
                            int64_t nr_bytes) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  auto position = bz_handle.seek_offset;
  auto remaining = bz_handle.data_size - position;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining);
  memcpy(buffer, bz_handle.data.get() + position, to_read);
  bz_handle.seek_offset += to_read;
  return UnsafeNumericCast<int64_t>(to_read);
}

int64_t Bz2FileSystem::GetFileSize(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  return UnsafeNumericCast<int64_t>(bz_handle.data_size);
}

void Bz2FileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  bz_handle.seek_offset = location;
}

void Bz2FileSystem::Reset(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  bz_handle.seek_offset = 0;
}

idx_t Bz2FileSystem::SeekPosition(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  return bz_handle.seek_offset;
}

bool Bz2FileSystem::CanSeek() { return true; }

timestamp_t Bz2FileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  auto &inner_handle = *bz_handle.inner_handle;
  return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType Bz2FileSystem::GetFileType(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  auto &inner_handle = *bz_handle.inner_handle;
  return inner_handle.file_system.GetFileType(inner_handle);
}

bool Bz2FileSystem::OnDiskFile(FileHandle &handle) {
  auto &bz_handle = handle.Cast<Bz2FileHandle>();
  return bz_handle.inner_handle->OnDiskFile();
}

vector<OpenFileInfo> Bz2FileSystem::Glob(const string &path,
                                         FileOpener *opener) {
  if (!opener) {
    return {};
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    return {};
  }
  auto &fs = FileSystem::GetFileSystem(*context);
  auto bz2_path = GetBz2FilePath(path);

  // Get matching bz2 files
  auto matching_files =
      fs.GlobFiles(bz2_path, *context, FileGlobOptions::DISALLOW_EMPTY);

  vector<OpenFileInfo> result;

  // Only add prefix if original path had a prefix
  if (HasBz2Prefix(path)) {
    string prefix = (path.substr(0, BZIP2_PREFIX.size()) == BZIP2_PREFIX)
                        ? BZIP2_PREFIX
                        : BZ2_PREFIX;
    for (const auto &file : matching_files) {
      result.push_back(prefix + file.path);
    }
  } else {
    // No prefix - return paths as-is (suffix-only case)
    for (const auto &file : matching_files) {
      result.push_back(file.path);
    }
  }

  return result;
}

bool Bz2FileSystem::FileExists(const string &filename,
                               optional_ptr<FileOpener> opener) {
  if (!opener) {
    return false;
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    return false;
  }
  auto bz2_path = GetBz2FilePath(filename);

  auto &fs = FileSystem::GetFileSystem(*context);
  return fs.FileExists(bz2_path);
}

} // namespace duckdb
