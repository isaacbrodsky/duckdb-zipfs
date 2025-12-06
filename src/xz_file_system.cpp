#include "xz_file_system.hpp"

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
make_uniq_array_xz(size_t n) {
  return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>(
      new DATA_TYPE[n]());
}

//------------------------------------------------------------------------------
// Xz File Handle
//------------------------------------------------------------------------------

void XzFileHandle::Close() { inner_handle->Close(); }

//------------------------------------------------------------------------------
// Xz File System
//------------------------------------------------------------------------------

static const string XZ_PREFIX = "xz://";
static const string LZMA_PREFIX = "lzma://";

// Check if path has xz:// or lzma:// prefix
static bool HasXzPrefix(const string &path) {
  return (path.size() > XZ_PREFIX.size() &&
          path.substr(0, XZ_PREFIX.size()) == XZ_PREFIX) ||
         (path.size() > LZMA_PREFIX.size() &&
          path.substr(0, LZMA_PREFIX.size()) == LZMA_PREFIX);
}

// Returns the actual file path (strips prefix if present)
static string GetXzFilePath(const string &path) {
  if (path.size() > XZ_PREFIX.size() &&
      path.substr(0, XZ_PREFIX.size()) == XZ_PREFIX) {
    return path.substr(XZ_PREFIX.size());
  }
  if (path.size() > LZMA_PREFIX.size() &&
      path.substr(0, LZMA_PREFIX.size()) == LZMA_PREFIX) {
    return path.substr(LZMA_PREFIX.size());
  }
  return path;
}

bool XzFileSystem::CanHandleFile(const string &fpath) {
  // Only handle files with xz:// or lzma:// prefix
  return HasXzPrefix(fpath);
}

unique_ptr<FileHandle> XzFileSystem::OpenFile(const string &path,
                                              FileOpenFlags flags,
                                              optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Xz file system can only open for reading");
  }

  // Get the path to the xz file (remove prefix if present)
  if (!opener) {
    throw IOException("Xz file system requires a file opener");
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    throw IOException("Xz file system requires a client context");
  }
  auto xz_path = GetXzFilePath(path);

  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(xz_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", xz_path);
  }

  // Read entire compressed file (may require multiple reads)
  idx_t compressed_size = handle->GetFileSize();
  auto compressed_data = make_uniq_array_xz<data_t>(compressed_size);
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

  // Decompress using liblzma
  // Start with 4x compressed size as initial estimate
  std::vector<data_t> decompressed;
  decompressed.reserve(compressed_size * 4);

  lzma_stream strm = LZMA_STREAM_INIT;
  lzma_ret ret;

  // Use multi-threaded decoder for files > 1MB (worth the overhead)
  // MT decoder works for both single-block and multi-block xz files
  constexpr size_t MT_THRESHOLD = 1024 * 1024; // 1MB
  bool use_mt = (total_read > MT_THRESHOLD);

  if (use_mt) {
    lzma_mt mt_options = {};
    mt_options.flags = 0;
    mt_options.block_size = 0; // Auto
    mt_options.timeout = 0;    // No timeout
    // Use 1/4 of physical memory for threading, unlimited for stop
    mt_options.memlimit_threading = lzma_physmem() / 4;
    mt_options.memlimit_stop = UINT64_MAX;
    // Use all available CPU threads
    mt_options.threads = lzma_cputhreads();
    if (mt_options.threads == 0) {
      mt_options.threads = 1;
    }

    ret = lzma_stream_decoder_mt(&strm, &mt_options);
  } else {
    // Single-threaded for small files
    ret = lzma_stream_decoder(&strm, UINT64_MAX, LZMA_CONCATENATED);
  }

  if (ret != LZMA_OK) {
    const char *msg;
    switch (ret) {
    case LZMA_MEM_ERROR:
      msg = "Memory allocation failed";
      break;
    case LZMA_OPTIONS_ERROR:
      msg = "Unsupported decompressor flags";
      break;
    default:
      msg = "Unknown error";
      break;
    }
    throw IOException("Failed to initialize xz decompression: %s (error %d)",
                      msg, ret);
  }

  strm.next_in = compressed_data.get();
  strm.avail_in = total_read;

  // Use larger chunks for better throughput (1MB)
  constexpr size_t CHUNK_SIZE = 1024 * 1024;
  std::vector<uint8_t> out_chunk(CHUNK_SIZE);

  lzma_action action = LZMA_RUN;

  while (true) {
    if (strm.avail_in == 0) {
      action = LZMA_FINISH;
    }

    strm.next_out = out_chunk.data();
    strm.avail_out = CHUNK_SIZE;

    ret = lzma_code(&strm, action);

    size_t have = CHUNK_SIZE - strm.avail_out;
    decompressed.insert(decompressed.end(), out_chunk.begin(),
                        out_chunk.begin() + have);

    if (ret == LZMA_STREAM_END) {
      break;
    }

    if (ret != LZMA_OK) {
      lzma_end(&strm);
      const char *msg;
      switch (ret) {
      case LZMA_MEM_ERROR:
        msg = "Memory allocation failed";
        break;
      case LZMA_FORMAT_ERROR:
        msg = "The input is not in the .xz format";
        break;
      case LZMA_OPTIONS_ERROR:
        msg = "Unsupported compression options";
        break;
      case LZMA_DATA_ERROR:
        msg = "Compressed file is corrupt";
        break;
      case LZMA_BUF_ERROR:
        msg = "Compressed file is truncated or corrupt";
        break;
      default:
        msg = "Unknown error";
        break;
      }
      throw IOException("Xz decompression error: %s (error %d)", msg, ret);
    }
  }

  lzma_end(&strm);

  // Copy to unique_ptr array
  idx_t data_size = decompressed.size();
  auto data = make_uniq_array_xz<data_t>(data_size);
  memcpy(data.get(), decompressed.data(), data_size);

  return make_uniq<XzFileHandle>(*this, path, flags, std::move(handle),
                                 std::move(data), data_size);
}

void XzFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                        idx_t location) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  auto remaining = xz_handle.data_size - location;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining);
  memcpy(buffer, xz_handle.data.get() + location, to_read);
}

int64_t XzFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  auto position = xz_handle.seek_offset;
  auto remaining = xz_handle.data_size - position;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining);
  memcpy(buffer, xz_handle.data.get() + position, to_read);
  xz_handle.seek_offset += to_read;
  return UnsafeNumericCast<int64_t>(to_read);
}

int64_t XzFileSystem::GetFileSize(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  return UnsafeNumericCast<int64_t>(xz_handle.data_size);
}

void XzFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  xz_handle.seek_offset = location;
}

void XzFileSystem::Reset(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  xz_handle.seek_offset = 0;
}

idx_t XzFileSystem::SeekPosition(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  return xz_handle.seek_offset;
}

bool XzFileSystem::CanSeek() { return true; }

timestamp_t XzFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  auto &inner_handle = *xz_handle.inner_handle;
  return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType XzFileSystem::GetFileType(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  auto &inner_handle = *xz_handle.inner_handle;
  return inner_handle.file_system.GetFileType(inner_handle);
}

bool XzFileSystem::OnDiskFile(FileHandle &handle) {
  auto &xz_handle = handle.Cast<XzFileHandle>();
  return xz_handle.inner_handle->OnDiskFile();
}

vector<OpenFileInfo> XzFileSystem::Glob(const string &path,
                                        FileOpener *opener) {
  if (!opener) {
    return {};
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    return {};
  }
  auto &fs = FileSystem::GetFileSystem(*context);
  auto xz_path = GetXzFilePath(path);

  // Get matching xz files
  auto matching_files =
      fs.GlobFiles(xz_path, *context, FileGlobOptions::DISALLOW_EMPTY);

  vector<OpenFileInfo> result;

  // Only add prefix if original path had a prefix
  if (HasXzPrefix(path)) {
    string prefix = (path.substr(0, LZMA_PREFIX.size()) == LZMA_PREFIX)
                        ? LZMA_PREFIX
                        : XZ_PREFIX;
    for (const auto &file : matching_files) {
      result.push_back(prefix + file.path);
    }
  } else {
    // No prefix - return paths as-is
    for (const auto &file : matching_files) {
      result.push_back(file.path);
    }
  }

  return result;
}

bool XzFileSystem::FileExists(const string &filename,
                              optional_ptr<FileOpener> opener) {
  if (!opener) {
    return false;
  }
  auto context = opener->TryGetClientContext();
  if (!context) {
    return false;
  }
  auto xz_path = GetXzFilePath(filename);

  auto &fs = FileSystem::GetFileSystem(*context);
  return fs.FileExists(xz_path);
}

} // namespace duckdb
