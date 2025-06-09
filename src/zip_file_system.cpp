#include "zip_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

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
// Zip File Handle
//------------------------------------------------------------------------------

void ZipFileHandle::Close() {}

//------------------------------------------------------------------------------
// Zip File System
//------------------------------------------------------------------------------

bool ZipFileSystem::CanHandleFile(const string &fpath) {
  // TODO: Check that we can seek into the file
  return fpath.size() > 6 && fpath.substr(0, 6) == "zip://";
}

/* Returns pointer and size of next block of data from archive. */
la_ssize_t FileSystemZipReadFunc(struct archive *archive, void *clientData,
                                 const void **buffer) {
  ZipArchiveHandle *handle = (ZipArchiveHandle *)clientData;
  auto readBytes =
      handle->inner_handle->Read(handle->data.get(), handle->data_len);
  *buffer = handle->data.get();
  return UnsafeNumericCast<la_ssize_t>(readBytes);
}

/* Seeks to specified location in the file and returns the position.
 * Whence values are SEEK_SET, SEEK_CUR, SEEK_END from stdio.h.
 * Return ARCHIVE_FATAL if the seek fails for any reason.
 */
la_int64_t FileSystemZipSeekFunc(struct archive *archive, void *clientData,
                                 la_int64_t offset, int whence) {
  ZipArchiveHandle *handle = (ZipArchiveHandle *)clientData;
  if (whence == SEEK_SET) {
    handle->inner_handle->Seek(offset);
  } else if (whence == SEEK_CUR) {
    handle->inner_handle->Seek(handle->inner_handle->SeekPosition() + offset);
  } else if (whence == SEEK_END) {
    handle->inner_handle->Seek(handle->inner_handle->GetFileSize() + offset);
  } else {
    return ARCHIVE_FATAL;
  }
  return ARCHIVE_OK;
}

int FileSystemZipOpenFunc(struct archive *archive, void *clientData) {
  return ARCHIVE_OK;
}

int FileSystemZipCloseFunc(struct archive *archive, void *clientData) {
  return ARCHIVE_OK;
}

unique_ptr<FileHandle>
ZipFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                        optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Zip file system can only open for reading");
  }

  // Get the path to the zip file
  auto context = opener->TryGetClientContext();
  const auto paths = SplitArchivePath(path.substr(6), *context);
  const auto &zip_path = paths.first;
  const auto &file_path = paths.second;

  // Now we need to find the file within the zip file and return out file handle
  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(zip_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", zip_path);
  }

  if (file_path.empty()) {
    return handle;
  }

  if (!handle->CanSeek()) {
    // TODO: Buffer?
    throw IOException("Cannot seek");
  }

  idx_t size = handle->GetFileSize();
  time_t last_modified_time = {0};
  bool has_last_modified_time = true;
  try {
    last_modified_time = fs.GetLastModifiedTime(*handle);
  } catch (NotImplementedException &ex) {
    has_last_modified_time = false;
  }
  auto file_type = fs.GetFileType(*handle);
  auto on_disk_file = handle->OnDiskFile();

  struct archive *archive = archive_read_new();
  try {
    if (archive_read_support_filter_all(archive)) {
      throw IOException("Failed to init libarchive (filter all): %s",
                        archive_error_string(archive));
    }
    if (archive_read_support_format_all(archive)) {
      throw IOException("Failed to init libarchive (format all): %s",
                        archive_error_string(archive));
    }
    unique_ptr<ZipArchiveHandle> zipHandle =
        make_uniq<ZipArchiveHandle>(std::move(handle));
    // TODO: Add skip?
    if (archive_read_set_seek_callback(archive, FileSystemZipSeekFunc)) {
      throw IOException("Failed to init libarchive (seek callback): %s",
                        archive_error_string(archive));
    }
    if (archive_read_open(archive, zipHandle.get(), &FileSystemZipOpenFunc,
                          &FileSystemZipReadFunc, &FileSystemZipCloseFunc)) {
      throw IOException("Failed to init libarchive (read callback): %s",
                        archive_error_string(archive));
    }
    struct archive_entry *entry = archive_entry_new2(archive);
    try {
      bool found = false;
      while (archive_read_next_header2(archive, entry) == ARCHIVE_OK) {
        auto pathName = archive_entry_pathname(entry);
        if (strcmp(pathName, file_path.c_str()) == 0) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw IOException("Failed to find file: %s", file_path);
      }

      auto read_buf_size = archive_entry_size(entry);
      auto read_buf = make_uniq_array2<data_t>(read_buf_size);

      auto read_bytes =
          archive_read_data(archive, read_buf.get(), read_buf_size);
      if (read_bytes < read_buf_size) {
        throw IOException("Failed to read: %s", archive_error_string(archive));
      }

      auto zip_file_handle = make_uniq<ZipFileHandle>(
          *this, path, flags, last_modified_time, has_last_modified_time,
          file_type, on_disk_file, read_buf_size, std::move(read_buf));

      archive_entry_free(entry);
      archive_read_free(archive);

      return zip_file_handle;
    } catch (Exception &ex2) {
      archive_entry_free(entry);
      throw;
    }
  } catch (Exception &ex) {
    archive_read_free(archive);
    throw;
  }
}

void ZipFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                         idx_t location) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  auto remaining_bytes = t_handle.sz - location;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  memcpy(buffer, t_handle.data.get() + location, to_read);
}

int64_t ZipFileSystem::Read(FileHandle &handle, void *buffer,
                            int64_t nr_bytes) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  auto position = t_handle.seek_offset;
  auto remaining_bytes = t_handle.sz - position;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  memcpy(buffer, t_handle.data.get() + position, to_read);
  t_handle.seek_offset += to_read;
  return to_read;
}

int64_t ZipFileSystem::GetFileSize(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return UnsafeNumericCast<int64_t>(t_handle.sz);
}

void ZipFileSystem::Seek(FileHandle &handle, idx_t location) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  t_handle.seek_offset = t_handle.seek_offset + location;
}

void ZipFileSystem::Reset(FileHandle &handle) { handle.Cast<ZipFileHandle>(); }

idx_t ZipFileSystem::SeekPosition(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return t_handle.seek_offset;
}

bool ZipFileSystem::CanSeek() { return true; }

time_t ZipFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  if (t_handle.has_last_modified_time) {
    return t_handle.last_modified_time;
  } else {
    throw NotImplementedException("ZipFileSystem: GetLastModifiedTime not "
                                  "implemented on underlying filesystem");
  }
}

FileType ZipFileSystem::GetFileType(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return t_handle.file_type;
}

bool ZipFileSystem::OnDiskFile(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return t_handle.on_disk_file;
}

vector<OpenFileInfo> ZipFileSystem::Glob(const string &path,
                                         FileOpener *opener) {
  // Remove the "zip://" prefix
  auto context = opener->TryGetClientContext();
  const auto parts = SplitArchivePath(path.substr(6), *context);
  auto &zip_path = parts.first;
  auto &file_path = parts.second;

  // Get matching zip files
  auto &fs = FileSystem::GetFileSystem(*context);
  vector<OpenFileInfo> matching_zips = fs.GlobFiles(zip_path, *context);

  Value zipfs_split_value = Value(LogicalType::VARCHAR);
  context->TryGetCurrentSetting("zipfs_split", zipfs_split_value);

  auto extension =
      !zipfs_split_value.IsNull() ? zipfs_split_value.GetValue<string>() : "";

  vector<OpenFileInfo> result;
  for (const auto &curr_zip : matching_zips) {
    if (!HasGlob(file_path)) {
      // No glob pattern in the file path, just return the file path
      result.push_back("zip://" + curr_zip.path + extension + "/" + file_path);
      continue;
    }

    auto pattern_parts = StringUtil::Split(file_path, '/');
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

    struct archive *archive = archive_read_new();
    try {
      if (archive_read_support_filter_all(archive)) {
        throw IOException("Failed to init libarchive (filter all): %s",
                          archive_error_string(archive));
      }
      if (archive_read_support_format_all(archive)) {
        throw IOException("Failed to init libarchive (format all): %s",
                          archive_error_string(archive));
      }
      unique_ptr<ZipArchiveHandle> zipHandle =
          make_uniq<ZipArchiveHandle>(std::move(archive_handle));
      // TODO: Add skip?
      if (archive_read_set_seek_callback(archive, FileSystemZipSeekFunc)) {
        throw IOException("Failed to init libarchive (seek callback): %s",
                          archive_error_string(archive));
      }
      if (archive_read_open(archive, zipHandle.get(), &FileSystemZipOpenFunc,
                            &FileSystemZipReadFunc, &FileSystemZipCloseFunc)) {
        throw IOException("Failed to init libarchive (read callback): %s",
                          archive_error_string(archive));
      }
      struct archive_entry *entry = archive_entry_new2(archive);
      try {
        string zip_filename;
        const size_t MAX_FILENAME_LEN = 65536; // = 2**16
        zip_filename.reserve(1024);

        while (archive_read_next_header2(archive, entry) == ARCHIVE_OK) {
          if (archive_entry_mode(entry) & AE_IFDIR) {
            continue;
          }

          if (archive_entry_is_encrypted(entry)) {
            continue;
          }

          auto path_name = archive_entry_pathname(entry);
          // TODO: May have backed out an optimization here
          zip_filename = path_name;

          auto entry_parts = StringUtil::Split(zip_filename, '/');

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
              // If the entry is deeper than the pattern (and we havent hit a
              // **), then it is not a match
              match = false;
              break;
            }
          }

          if (match) {
            auto entry_path =
                "zip://" + curr_zip.path + extension + "/" + zip_filename;
            // Cache here???
            result.push_back(entry_path);
          }
        }

        archive_entry_free(entry);
        archive_read_free(archive);
      } catch (Exception &ex2) {
        archive_entry_free(entry);
        throw;
      }
    } catch (Exception &ex) {
      archive_read_free(archive);
      throw;
    }
  }

  return result;
}

} // namespace duckdb