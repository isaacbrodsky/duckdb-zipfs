#include "zip_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

namespace {

auto const ZIP_SEPARATOR = "/";
static constexpr uint32_t ZIP_LOCAL_HEADER_SIGNATURE = 0x04034b50;
static constexpr idx_t ZIP_LOCAL_HEADER_SIZE = 30;

static int64_t GetZipfsBigintSetting(ClientContext &context,
                                     const string &setting_name,
                                     int64_t default_value) {
  Value value = Value::BIGINT(default_value);
  context.TryGetCurrentSetting(setting_name, value);
  if (value.IsNull()) {
    return default_value;
  }
  return value.GetValue<int64_t>();
}

static uint16_t ReadLE16(const uint8_t *ptr) {
  return UnsafeNumericCast<uint16_t>(ptr[0] | (ptr[1] << 8));
}

static uint32_t ReadLE32(const uint8_t *ptr) {
  return UnsafeNumericCast<uint32_t>(ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) |
                                     (ptr[3] << 24));
}

static int64_t GetMemberDataOffset(FileHandle &outer_handle,
                                   const mz_zip_archive_file_stat &file_stat) {
  uint8_t local_header[ZIP_LOCAL_HEADER_SIZE];
  outer_handle.Read(local_header, ZIP_LOCAL_HEADER_SIZE,
                    UnsafeNumericCast<idx_t>(file_stat.m_local_header_ofs));

  if (ReadLE32(local_header) != ZIP_LOCAL_HEADER_SIGNATURE) {
    throw IOException("Invalid ZIP local header signature for member: %s",
                      file_stat.m_filename);
  }

  auto filename_len = ReadLE16(local_header + 26);
  auto extra_len = ReadLE16(local_header + 28);
  return UnsafeNumericCast<int64_t>(file_stat.m_local_header_ofs) +
         UnsafeNumericCast<int64_t>(ZIP_LOCAL_HEADER_SIZE) + filename_len +
         extra_len;
}

} // namespace

//------------------------------------------------------------------------------
// Zip Utilities
//------------------------------------------------------------------------------

// Split a zip path into the path to the archive and the path within the archive
static pair<string, string> SplitArchivePath(const string &path,
                                             ClientContext &context) {
  Value zipfs_split_value = Value(LogicalType::VARCHAR);
  context.TryGetCurrentSetting("zipfs_split", zipfs_split_value);

  if (!zipfs_split_value.IsNull()) {
    auto zipfs_split_str = zipfs_split_value.GetValue<string>();

    const auto zip_path =
        std::search(path.begin(), path.end(), zipfs_split_str.begin(),
                    zipfs_split_str.end());

    const auto suffix_found = zip_path != path.end();
    const auto suffix_path =
        suffix_found
            ? zip_path + UnsafeNumericCast<int64_t>(zipfs_split_str.size())
            : zip_path;

    if (suffix_path == path.end()) {
      // Glob entire zip file by default
      return {string(path.begin(),
                     path.end() - (suffix_found ? zipfs_split_str.size() : 0)),
              "**"};
    }

    // If there is a slash after the last .zip, we need to remove everything
    // after that
    auto archive_path =
        string(path.begin(),
               suffix_path - (suffix_found ? zipfs_split_str.size() : 0));
    auto file_path =
        string(suffix_path + (*suffix_path == '/' ? 1 : 0), path.end());
    return {archive_path, file_path};
  } else {
    Value zipfs_extension_value = ".zip";
    context.TryGetCurrentSetting("zipfs_extension", zipfs_extension_value);

    auto zipfs_extension_str = zipfs_extension_value.GetValue<string>();

    const auto zip_path =
        std::search(path.begin(), path.end(), zipfs_extension_str.begin(),
                    zipfs_extension_str.end());

    if (zip_path == path.end()) {
      throw IOException("Could not find a '%s' archive to open in: '%s'",
                        zipfs_extension_str.c_str(), path);
    }

    const auto suffix_path =
        zip_path + UnsafeNumericCast<int64_t>(zipfs_extension_str.size());

    if (suffix_path == path.end()) {
      // Glob entire zip file by default
      return {path, "**"};
    }

    if (*suffix_path == '/') {
      // If there is a slash after the last .zip, we need to remove everything
      // after that
      auto archive_path = string(path.begin(), suffix_path);
      auto file_path = string(suffix_path + 1, path.end());
      return {archive_path, file_path};
    }

    throw IOException(
        "Could not find valid path within '%s' archive to open in: '%s'",
        zipfs_extension_str.c_str(), path);
  }
}

//------------------------------------------------------------------------------
// Common miniz callback
//------------------------------------------------------------------------------

size_t FileSystemZipReadFunc(void *pOpaque, mz_uint64 file_ofs, void *pBuf,
                             size_t n) {
  FileHandle *handle = (FileHandle *)pOpaque;
  handle->Read(pBuf, UnsafeNumericCast<idx_t>(n),
               UnsafeNumericCast<idx_t>(file_ofs));
  return UnsafeNumericCast<size_t>(n);
}

//------------------------------------------------------------------------------
// Zip File System
//------------------------------------------------------------------------------

bool ZipFileSystem::CanHandleFile(const string &fpath) {
  return fpath.size() > 6 && fpath.substr(0, 6) == "zip://";
}

unique_ptr<FileHandle>
ZipFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                        optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Zip file system can only open for reading");
  }

  auto context = opener->TryGetClientContext();
  const auto paths = SplitArchivePath(path.substr(6), *context);
  const auto &zip_path = paths.first;
  const auto &file_path = paths.second;

  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(zip_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", zip_path);
  }

  if (file_path.empty()) {
    return handle;
  }

  auto normalized_file_path = StringUtil::Replace(
      file_path, fs.PathSeparator(file_path), ZIP_SEPARATOR);

  if (!handle->CanSeek()) {
    throw IOException("Cannot seek");
  }

  auto seek_threshold = MaxValue<int64_t>(
      0, GetZipfsBigintSetting(*context, "zipfs_seek_threshold", 268435456));
  auto zran_span = MaxValue<int64_t>(
      1, GetZipfsBigintSetting(*context, "zipfs_zran_span", 1048576));

  idx_t size = handle->GetFileSize();

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();
  try {
    mz_uint zip_flags = 0;

    if (!mz_zip_reader_init(&zip, size, zip_flags)) {
      throw IOException("Could not open as zip file: %s",
                        mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }

    mz_uint file_index = 0;
    auto locate_failed =
        mz_zip_reader_locate_file_v2(&zip, normalized_file_path.c_str(),
                                     nullptr, 0, &file_index) == MZ_FALSE;
    if (locate_failed) {
      throw IOException("Failed to find file: %s", normalized_file_path);
    }

    mz_zip_archive_file_stat file_stat = {0};
    auto stat_failed =
        mz_zip_reader_file_stat(&zip, file_index, &file_stat) == MZ_FALSE;

    if (stat_failed) {
      throw IOException("Problem stat-ing file within archive: %s",
                        mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }
    if ((file_stat.m_method) && (file_stat.m_method != MZ_DEFLATED)) {
      throw IOException("Unsupported compression method: %u",
                        file_stat.m_method);
    }

    auto data_offset = GetMemberDataOffset(*handle, file_stat);

    if (file_stat.m_method == 0) {
      auto zip_file_handle = make_uniq<StoredMemberHandle>(
          *this, path, flags, std::move(handle), file_stat, data_offset);
      mz_zip_reader_end(&zip);
      return zip_file_handle;
    }

    if (file_stat.m_uncomp_size <=
        UnsafeNumericCast<mz_uint64>(seek_threshold)) {
      vector<data_t> read_buf;
      read_buf.resize(UnsafeNumericCast<size_t>(file_stat.m_uncomp_size));
      if (file_stat.m_uncomp_size > 0 &&
          !mz_zip_reader_extract_to_mem(&zip, file_index, read_buf.data(),
                                        read_buf.size(), 0)) {
        throw IOException("Problem extracting file within archive: %s",
                          mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
      }

      auto zip_file_handle =
          make_uniq<BufferedMemberHandle>(*this, path, flags, std::move(handle),
                                          file_stat, std::move(read_buf));
      mz_zip_reader_end(&zip);
      return zip_file_handle;
    }

    auto index = DeflateIndex::Build(
        *handle, data_offset, UnsafeNumericCast<int64_t>(file_stat.m_comp_size),
        zran_span);
    auto zip_file_handle = make_uniq<ZranMemberHandle>(
        *this, path, flags, std::move(handle), file_stat, std::move(index));

    mz_zip_reader_end(&zip);
    return zip_file_handle;
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }
}

void ZipFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                         idx_t location) {
  if (nr_bytes <= 0) {
    return;
  }
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  auto read_bytes = t_handle.ReadAt(buffer, nr_bytes, location);
  if (read_bytes != nr_bytes) {
    throw IOException(
        "Failed to read %lld bytes from zip member at offset %llu",
        UnsafeNumericCast<long long>(nr_bytes),
        UnsafeNumericCast<unsigned long long>(location));
  }
}

int64_t ZipFileSystem::Read(FileHandle &handle, void *buffer,
                            int64_t nr_bytes) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  return t_handle.Read(buffer, nr_bytes);
}

int64_t ZipFileSystem::GetFileSize(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  return t_handle.GetFileSize();
}

void ZipFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  t_handle.Seek(location);
}

void ZipFileSystem::Reset(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  t_handle.Reset();
}

idx_t ZipFileSystem::SeekPosition(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  return t_handle.SeekPosition();
}

bool ZipFileSystem::CanSeek() { return true; }

timestamp_t ZipFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  return t_handle.GetLastModifiedTime();
}

FileType ZipFileSystem::GetFileType(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipMemberHandle>();
  return t_handle.GetFileType();
}

bool ZipFileSystem::OnDiskFile(FileHandle &handle) { return false; }

vector<OpenFileInfo> ZipFileSystem::Glob(const string &path,
                                         FileOpener *opener) {
  // Remove the "zip://" prefix
  auto context = opener->TryGetClientContext();
  auto &fs = FileSystem::GetFileSystem(*context);
  const auto parts = SplitArchivePath(path.substr(6), *context);
  auto &zip_path = parts.first;
  const auto has_glob = HasGlob(zip_path);
  auto &file_path = parts.second;

  // Get matching zip files
  vector<OpenFileInfo> matching_zips;
  if (has_glob) {
    matching_zips = fs.GlobFiles(zip_path, FileGlobOptions::DISALLOW_EMPTY);
  } else {
    // Normally, GlobFiles would be safe. However, when
    // there is no glob, we don't call it because it can mangle https:// URLs
    // (converting slashes into backslashes.)
    matching_zips = {OpenFileInfo(zip_path)};
  }

  Value zipfs_split_value = Value(LogicalType::VARCHAR);
  context->TryGetCurrentSetting("zipfs_split", zipfs_split_value);

  auto extension =
      !zipfs_split_value.IsNull() ? zipfs_split_value.GetValue<string>() : "";

  vector<OpenFileInfo> result;
  for (const auto &curr_zip : matching_zips) {
    if (!HasGlob(file_path)) {
      // No glob pattern in the file path, just return the file path
      result.push_back("zip://" + curr_zip.path + extension + ZIP_SEPARATOR +
                       file_path);
      continue;
    }

    auto pattern_parts = StringUtil::Split(file_path, ZIP_SEPARATOR);
    // TODO: We may want to detect globbing into a nested zip file and reject.

    // Given the path to the zip file, open it
    auto archive_handle = fs.OpenFile(curr_zip, FileFlags::FILE_FLAGS_READ);
    if (!archive_handle) {
      continue; // Skip invalid zip files
    }
    if (!archive_handle->CanSeek()) {
      continue; // Skip unseekable files
    }

    idx_t size = archive_handle->GetFileSize();

    mz_zip_archive zip;
    mz_zip_zero_struct(&zip);
    zip.m_pRead = &FileSystemZipReadFunc;
    zip.m_pIO_opaque = archive_handle.get();

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

        if (mz_zip_reader_is_file_a_directory(&zip, i))
          continue;

        mz_zip_validate_file(&zip, i, MZ_ZIP_FLAG_VALIDATE_HEADERS_ONLY);

        if (mz_zip_reader_is_file_encrypted(&zip, i))
          continue;

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

        auto entry_parts = StringUtil::Split(zip_filename, ZIP_SEPARATOR);

        if (entry_parts.size() < pattern_parts.size()) {
          // This entry is not deep enough to match the pattern
          continue;
        }

        // Check if the pattern matches the entry
        bool match = true;
        for (idx_t i = 0; i < pattern_parts.size(); i++) {
          const auto &pp = pattern_parts[i];
          const auto &ep = entry_parts[i];

          if (pp == "**") {
            // We only allow crawl's to be at the end of the pattern
            if (i != pattern_parts.size() - 1) {
              throw NotImplementedException(
                  "Recursive globs are only supported at the end of zip file "
                  "path patterns");
            }
            // Otherwise, everything else is a match
            match = true;
            break;
          }

          if (!duckdb::Glob(ep.c_str(), ep.size(), pp.c_str(), pp.size())) {
            // Not a match
            match = false;
            break;
          }

          if (i == pattern_parts.size() - 1 &&
              entry_parts.size() > pattern_parts.size()) {
            // If the entry is deeper than the pattern (and we havent hit a **),
            // then it is not a match
            match = false;
            break;
          }
        }

        if (match) {
          auto entry_path = "zip://" + curr_zip.path + extension +
                            ZIP_SEPARATOR + zip_filename;
          result.push_back(entry_path);
        }
      }

      mz_zip_reader_end(&zip);
    } catch (Exception &ex) {
      mz_zip_reader_end(&zip);
      throw;
    }
  }

  return result;
}

bool ZipFileSystem::FileExists(const string &filename,
                               optional_ptr<FileOpener> opener) {
  // Remove the "zip://" prefix
  auto context = opener->TryGetClientContext();
  const auto parts = SplitArchivePath(filename.substr(6), *context);
  auto &zip_path = parts.first;
  auto &file_path = parts.second;

  auto &fs = FileSystem::GetFileSystem(*context);
  // Do not pass opener here, as it will crash later.
  if (!fs.FileExists(zip_path)) {
    return false;
  }

  auto normalized_file_path = StringUtil::Replace(
      file_path, fs.PathSeparator(file_path), ZIP_SEPARATOR);

  auto handle = fs.OpenFile(zip_path, FileOpenFlags::FILE_FLAGS_READ);
  if (!handle) {
    return false;
  }

  if (!handle->CanSeek()) {
    return false;
  }

  idx_t size = handle->GetFileSize();

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();
  try {
    mz_uint zip_flags = 0;

    if (!mz_zip_reader_init(&zip, size, zip_flags)) {
      return false;
    }

    mz_uint file_index = 0;
    auto locate_failed =
        mz_zip_reader_locate_file_v2(&zip, normalized_file_path.c_str(),
                                     nullptr, 0, &file_index) == MZ_FALSE;
    if (locate_failed) {
      return false;
    }

    mz_zip_archive_file_stat file_stat = {0};
    auto stat_failed =
        mz_zip_reader_file_stat(&zip, file_index, &file_stat) == MZ_FALSE;

    if (stat_failed) {
      return false;
    }
    if ((file_stat.m_method) && (file_stat.m_method != MZ_DEFLATED)) {
      return false;
    }

    mz_zip_reader_end(&zip);

    return true;
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }
}

//------------------------------------------------------------------------------
// Zip Stream File Handle
//------------------------------------------------------------------------------

ZipStreamFileHandle::~ZipStreamFileHandle() {
  try {
    Close();
  } catch (...) { // NOLINT
  }
}

void ZipStreamFileHandle::Close() {
  if (closed) {
    return;
  }
  closed = true;

  if (iter_state) {
    mz_zip_reader_extract_iter_free(iter_state);
    iter_state = nullptr;
  }

  if (zip.m_pState) {
    mz_zip_reader_end(&zip);
    mz_zip_zero_struct(&zip);
  }

  if (inner_handle) {
    inner_handle->Close();
  }
}

//------------------------------------------------------------------------------
// Zip Stream File System
//------------------------------------------------------------------------------

bool ZipStreamFileSystem::CanHandleFile(const string &fpath) {
  // This only checks the URL scheme. We still validate seekability of the
  // underlying zip archive in OpenFile(). Even though zipstream:// exposes the
  // selected member as a sequential stream, the outer .zip file itself must be
  // seekable so miniz can read the central directory and member data.
  return fpath.size() > 12 && fpath.substr(0, 12) == "zipstream://";
}

unique_ptr<FileHandle>
ZipStreamFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                              optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Zip stream file system can only open for reading");
  }

  // Get the path to the zip file
  auto context = opener->TryGetClientContext();
  const auto paths = SplitArchivePath(path.substr(12), *context);
  const auto &zip_path = paths.first;
  const auto &file_path = paths.second;

  // Now we need to find the file within the zip file and return our file handle
  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(zip_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", zip_path);
  }

  if (file_path.empty()) {
    return handle;
  }

  auto normalized_file_path = StringUtil::Replace(
      file_path, fs.PathSeparator(file_path), ZIP_SEPARATOR);

  if (!handle->CanSeek()) {
    throw IOException("Cannot seek");
  }

  idx_t size = handle->GetFileSize();

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();
  mz_zip_reader_extract_iter_state *iter_state = nullptr;
  try {
    mz_uint zip_flags = 0;

    if (!mz_zip_reader_init(&zip, size, zip_flags)) {
      throw IOException("Could not open as zip file: %s",
                        mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }

    mz_uint file_index = 0;
    auto locate_failed =
        mz_zip_reader_locate_file_v2(&zip, normalized_file_path.c_str(),
                                     nullptr, 0, &file_index) == MZ_FALSE;
    if (locate_failed) {
      throw IOException("Failed to find file: %s", normalized_file_path);
    }

    mz_zip_archive_file_stat file_stat = {0};
    auto stat_failed =
        mz_zip_reader_file_stat(&zip, file_index, &file_stat) == MZ_FALSE;

    if (stat_failed) {
      throw IOException("Problem stat-ing file within archive: %s",
                        mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }
    if ((file_stat.m_method) && (file_stat.m_method != MZ_DEFLATED)) {
      throw IOException("Unknown compression method");
    }

    iter_state = mz_zip_reader_extract_iter_new(&zip, file_index, 0);
    if (!iter_state) {
      throw IOException(
          "Problem creating zip stream for file within archive: %s",
          mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
    }

    auto zip_stream_handle = make_uniq<ZipStreamFileHandle>(
        *this, path, flags, std::move(handle), file_stat, file_index);
    zip_stream_handle->zip = zip;
    mz_zip_zero_struct(&zip);
    zip_stream_handle->iter_state = iter_state;
    // miniz stores the mz_zip_archive pointer that was passed to
    // mz_zip_reader_extract_iter_new() inside the iterator state. The iterator
    // was created against the local stack variable above, but from this point
    // on the archive is owned by the ZipStreamFileHandle. Retarget the iterator
    // to the archive copy in the handle; otherwise reads dereference a dangling
    // stack pointer after OpenFile() returns.
    zip_stream_handle->iter_state->pZip = &zip_stream_handle->zip;
    iter_state = nullptr;

    return zip_stream_handle;
  } catch (Exception &ex) {
    if (iter_state) {
      mz_zip_reader_extract_iter_free(iter_state);
    }
    mz_zip_reader_end(&zip);
    throw;
  }
}

void ZipStreamFileSystem::Read(FileHandle &handle, void *buffer,
                               int64_t nr_bytes, idx_t location) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  auto original_position = t_handle.seek_offset;

  try {
    Seek(handle, location);
    Read(handle, buffer, nr_bytes);
    Seek(handle, original_position);
  } catch (Exception &ex) {
    try {
      Seek(handle, original_position);
    } catch (...) { // NOLINT
    }
    throw;
  }
}

int64_t ZipStreamFileSystem::Read(FileHandle &handle, void *buffer,
                                  int64_t nr_bytes) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  auto remaining_bytes =
      t_handle.file_stat.m_uncomp_size - t_handle.seek_offset;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  if (to_read == 0) {
    return 0;
  }

  auto read_bytes =
      mz_zip_reader_extract_iter_read(t_handle.iter_state, buffer, to_read);
  if (read_bytes == 0 && to_read > 0) {
    auto err = mz_zip_get_last_error(&t_handle.zip);
    if (err != MZ_ZIP_NO_ERROR) {
      throw IOException("Problem reading file within archive: %s",
                        mz_zip_get_error_string(err));
    }
  }

  t_handle.seek_offset += UnsafeNumericCast<idx_t>(read_bytes);
  return UnsafeNumericCast<int64_t>(read_bytes);
}

int64_t ZipStreamFileSystem::GetFileSize(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  return UnsafeNumericCast<int64_t>(t_handle.file_stat.m_uncomp_size);
}

void ZipStreamFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  auto file_size = UnsafeNumericCast<idx_t>(t_handle.file_stat.m_uncomp_size);
  auto target_location = MinValue(location, file_size);

  if (target_location < t_handle.seek_offset) {
    Reset(handle);
  }

  if (target_location == t_handle.seek_offset) {
    return;
  }

  data_t skip_buffer[8192];
  while (t_handle.seek_offset < target_location) {
    auto to_skip =
        MinValue<idx_t>(target_location - t_handle.seek_offset,
                        UnsafeNumericCast<idx_t>(sizeof(skip_buffer)));
    auto skipped =
        Read(handle, skip_buffer, UnsafeNumericCast<int64_t>(to_skip));
    if (skipped <= 0) {
      throw IOException("Failed to seek within zip stream");
    }
  }
}

void ZipStreamFileSystem::Reset(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  if (t_handle.iter_state) {
    mz_zip_reader_extract_iter_free(t_handle.iter_state);
    t_handle.iter_state = nullptr;
  }

  auto *iter_state =
      mz_zip_reader_extract_iter_new(&t_handle.zip, t_handle.file_index, 0);
  if (!iter_state) {
    throw IOException(
        "Problem creating zip stream for file within archive: %s",
        mz_zip_get_error_string(mz_zip_get_last_error(&t_handle.zip)));
  }

  t_handle.iter_state = iter_state;
  t_handle.seek_offset = 0;
}

idx_t ZipStreamFileSystem::SeekPosition(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  return t_handle.seek_offset;
}

bool ZipStreamFileSystem::CanSeek() {
  // We intentionally do not advertise seek support. Some internal operations
  // can still replay from the start of the member via Seek()/Reset(), but
  // callers should treat zipstream:// as a sequential stream.
  return false;
}

timestamp_t ZipStreamFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  auto &inner_handle = *t_handle.inner_handle;
  return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType ZipStreamFileSystem::GetFileType(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  auto &inner_handle = *t_handle.inner_handle;
  return inner_handle.file_system.GetFileType(inner_handle);
}

bool ZipStreamFileSystem::OnDiskFile(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipStreamFileHandle>();
  return t_handle.inner_handle->OnDiskFile();
}

vector<OpenFileInfo> ZipStreamFileSystem::Glob(const string &path,
                                               FileOpener *opener) {
  // Remove the "zipstream://" prefix
  auto context = opener->TryGetClientContext();
  auto &fs = FileSystem::GetFileSystem(*context);
  const auto parts = SplitArchivePath(path.substr(12), *context);
  auto &zip_path = parts.first;
  const auto has_glob = HasGlob(zip_path);
  auto &file_path = parts.second;

  // Get matching zip files
  vector<OpenFileInfo> matching_zips;
  if (has_glob) {
    matching_zips = fs.GlobFiles(zip_path, FileGlobOptions::DISALLOW_EMPTY);
  } else {
    // Normally, GlobFiles would be safe. However, when
    // there is no glob, we don't call it because it can mangle https:// URLs
    // (converting slashes into backslashes.)
    matching_zips = {OpenFileInfo(zip_path)};
  }

  Value zipfs_split_value = Value(LogicalType::VARCHAR);
  context->TryGetCurrentSetting("zipfs_split", zipfs_split_value);

  auto extension =
      !zipfs_split_value.IsNull() ? zipfs_split_value.GetValue<string>() : "";

  vector<OpenFileInfo> result;
  for (const auto &curr_zip : matching_zips) {
    if (!HasGlob(file_path)) {
      // No glob pattern in the file path, just return the file path
      result.push_back("zipstream://" + curr_zip.path + extension +
                       ZIP_SEPARATOR + file_path);
      continue;
    }

    auto pattern_parts = StringUtil::Split(file_path, ZIP_SEPARATOR);
    // TODO: We may want to detect globbing into a nested zip file and reject.

    // Given the path to the zip file, open it
    auto archive_handle = fs.OpenFile(curr_zip, FileFlags::FILE_FLAGS_READ);
    if (!archive_handle) {
      continue; // Skip invalid zip files
    }
    if (!archive_handle->CanSeek()) {
      continue; // Skip unseekable files
    }

    idx_t size = archive_handle->GetFileSize();

    mz_zip_archive zip;
    mz_zip_zero_struct(&zip);
    zip.m_pRead = &FileSystemZipReadFunc;
    zip.m_pIO_opaque = archive_handle.get();

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

        if (mz_zip_reader_is_file_a_directory(&zip, i))
          continue;

        mz_zip_validate_file(&zip, i, MZ_ZIP_FLAG_VALIDATE_HEADERS_ONLY);

        if (mz_zip_reader_is_file_encrypted(&zip, i))
          continue;

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

        auto entry_parts = StringUtil::Split(zip_filename, ZIP_SEPARATOR);

        if (entry_parts.size() < pattern_parts.size()) {
          // This entry is not deep enough to match the pattern
          continue;
        }

        // Check if the pattern matches the entry
        bool match = true;
        for (idx_t i = 0; i < pattern_parts.size(); i++) {
          const auto &pp = pattern_parts[i];
          const auto &ep = entry_parts[i];

          if (pp == "**") {
            // We only allow crawl's to be at the end of the pattern
            if (i != pattern_parts.size() - 1) {
              throw NotImplementedException(
                  "Recursive globs are only supported at the end of zip file "
                  "path patterns");
            }
            // Otherwise, everything else is a match
            match = true;
            break;
          }

          if (!duckdb::Glob(ep.c_str(), ep.size(), pp.c_str(), pp.size())) {
            // Not a match
            match = false;
            break;
          }

          if (i == pattern_parts.size() - 1 &&
              entry_parts.size() > pattern_parts.size()) {
            // If the entry is deeper than the pattern (and we havent hit a **),
            // then it is not a match
            match = false;
            break;
          }
        }

        if (match) {
          auto entry_path = "zipstream://" + curr_zip.path + extension +
                            ZIP_SEPARATOR + zip_filename;
          result.push_back(entry_path);
        }
      }

      mz_zip_reader_end(&zip);
    } catch (Exception &ex) {
      mz_zip_reader_end(&zip);
      throw;
    }
  }

  return result;
}

bool ZipStreamFileSystem::FileExists(const string &filename,
                                     optional_ptr<FileOpener> opener) {
  // Remove the "zipstream://" prefix
  auto context = opener->TryGetClientContext();
  const auto parts = SplitArchivePath(filename.substr(12), *context);
  auto &zip_path = parts.first;
  auto &file_path = parts.second;

  auto &fs = FileSystem::GetFileSystem(*context);
  // Do not pass opener here, as it will crash later.
  if (!fs.FileExists(zip_path)) {
    return false;
  }

  auto normalized_file_path = StringUtil::Replace(
      file_path, fs.PathSeparator(file_path), ZIP_SEPARATOR);

  auto handle = fs.OpenFile(zip_path, FileOpenFlags::FILE_FLAGS_READ);
  if (!handle) {
    return false;
  }

  if (!handle->CanSeek()) {
    return false;
  }

  idx_t size = handle->GetFileSize();

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();
  try {
    mz_uint zip_flags = 0;

    if (!mz_zip_reader_init(&zip, size, zip_flags)) {
      return false;
    }

    mz_uint file_index = 0;
    auto locate_failed =
        mz_zip_reader_locate_file_v2(&zip, normalized_file_path.c_str(),
                                     nullptr, 0, &file_index) == MZ_FALSE;
    if (locate_failed) {
      return false;
    }

    mz_zip_archive_file_stat file_stat = {0};
    auto stat_failed =
        mz_zip_reader_file_stat(&zip, file_index, &file_stat) == MZ_FALSE;

    if (stat_failed) {
      return false;
    }
    if ((file_stat.m_method) && (file_stat.m_method != MZ_DEFLATED)) {
      return false;
    }

    mz_zip_reader_end(&zip);

    return true;
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }
}

} // namespace duckdb
