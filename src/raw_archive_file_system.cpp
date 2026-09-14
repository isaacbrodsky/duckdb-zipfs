#include "archive_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

#ifdef ENABLE_LIBARCHIVE

namespace duckdb {

bool RawArchiveFileSystem::CanHandleFile(const string &fpath) {
  // TODO: Check that we can seek into the file
  return fpath.size() > 13 && fpath.substr(0, 13) == "compressed://";
}

unique_ptr<FileHandle>
RawArchiveFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                               optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Archive file system can only open for reading");
  }

  // Get the path to the zip file
  auto context = opener->TryGetClientContext();
  const auto file_path = path.substr(13);

  // Now we need to find the file within the zip file and return out file handle
  auto &fs = FileSystem::GetFileSystem(*context);
  auto handle = fs.OpenFile(file_path, flags);
  if (!handle) {
    throw IOException("Failed to open file: %s", file_path);
  }

  if (!handle->CanSeek()) {
    // TODO: Buffer?
    throw IOException("Cannot seek");
  }

  timestamp_t last_modified_time;
  bool has_last_modified_time = true;
  try {
    last_modified_time = fs.GetLastModifiedTime(*handle);
  } catch (NotImplementedException &ex) {
    has_last_modified_time = false;
  }
  auto file_type = fs.GetFileType(*handle);
  auto on_disk_file = handle->OnDiskFile();

  auto lib_handle = make_uniq<LibArchiveHandle>(std::move(handle));
  auto archive_file_handle = make_uniq<ArchiveFileHandle>(
      *this, path, flags, last_modified_time, has_last_modified_time, file_type,
      on_disk_file, std::move(lib_handle), /*raw_format=*/true,
      /*entry_name=*/"");
  // Open the decompression stream now so an unreadable file fails fast.
  archive_file_handle->InitStream();
  return archive_file_handle;
}

void RawArchiveFileSystem::Read(FileHandle &handle, void *buffer,
                                int64_t nr_bytes, idx_t location) {
  handle.Cast<ArchiveFileHandle>().ReadBytesAt(buffer, nr_bytes, location);
}

int64_t RawArchiveFileSystem::Read(FileHandle &handle, void *buffer,
                                   int64_t nr_bytes) {
  return handle.Cast<ArchiveFileHandle>().ReadBytes(buffer, nr_bytes);
}

int64_t RawArchiveFileSystem::GetFileSize(FileHandle &handle) {
  return handle.Cast<ArchiveFileHandle>().Size();
}

void RawArchiveFileSystem::Seek(FileHandle &handle, idx_t location) {
  handle.Cast<ArchiveFileHandle>().seek_offset = location;
}

void RawArchiveFileSystem::Reset(FileHandle &handle) {
  handle.Cast<ArchiveFileHandle>().seek_offset = 0;
}

idx_t RawArchiveFileSystem::SeekPosition(FileHandle &handle) {
  return handle.Cast<ArchiveFileHandle>().seek_offset;
}

bool RawArchiveFileSystem::CanSeek() { return true; }

timestamp_t RawArchiveFileSystem::GetLastModifiedTime(FileHandle &handle) {
  auto &t_handle = handle.Cast<ArchiveFileHandle>();
  if (t_handle.has_last_modified_time) {
    return t_handle.last_modified_time;
  } else {
    throw NotImplementedException(
        "RawArchiveFileSystem: GetLastModifiedTime not "
        "implemented on underlying filesystem");
  }
}

FileType RawArchiveFileSystem::GetFileType(FileHandle &handle) {
  auto &t_handle = handle.Cast<ArchiveFileHandle>();
  return t_handle.file_type;
}

bool RawArchiveFileSystem::OnDiskFile(FileHandle &handle) {
  auto &t_handle = handle.Cast<ArchiveFileHandle>();
  return t_handle.on_disk_file;
}

vector<OpenFileInfo> RawArchiveFileSystem::Glob(const string &path,
                                                FileOpener *opener) {
  // Remove the "compressed://" prefix
  auto context = opener->TryGetClientContext();
  auto &fs = FileSystem::GetFileSystem(*context);
  const auto file_path = path.substr(13);
  auto has_glob = HasGlob(file_path);

  // Get matching zip files
  if (has_glob) {
    auto matching_zips =
        fs.GlobFiles(file_path, FileGlobOptions::DISALLOW_EMPTY);
    // TODO: Rewrite to include compressed:// prefix

    vector<OpenFileInfo> result;
    for (const auto &curr_zip : matching_zips) {
      result.push_back("compressed://" + curr_zip.path);
    }
    return result;
  } else {
    // Normally, GlobFiles would be safe. However, when
    // there is no glob, we don't call it because it can mangle https:// URLs
    // (converting slashes into backslashes.)
    return {OpenFileInfo(path)};
  }
}

bool RawArchiveFileSystem::FileExists(const string &filename,
                                      optional_ptr<FileOpener> opener) {
  // Remove the "compressed://" prefix
  auto context = opener->TryGetClientContext();
  const auto file_path = filename.substr(13);

  auto &fs = FileSystem::GetFileSystem(*context);
  // Do not pass opener here, as it will crash later.
  if (!fs.FileExists(file_path)) {
    return false;
  }

  auto handle = fs.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
  if (!handle) {
    return false;
  }

  if (!handle->CanSeek()) {
    // TODO: Buffer?
    return false;
  }

  struct archive *archive = archive_read_new();
  try {
    if (archive_read_support_filter_all(archive)) {
      throw IOException("Failed to init libarchive (filter all): %s",
                        archive_error_string(archive));
    }
    if (archive_read_support_format_raw(archive)) {
      throw IOException("Failed to init libarchive (format raw): %s",
                        archive_error_string(archive));
    }
    unique_ptr<LibArchiveHandle> zipHandle =
        make_uniq<LibArchiveHandle>(std::move(handle));
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

      if (archive_read_next_header2(archive, entry) == ARCHIVE_OK) {
        found = true;
      }

      archive_entry_free(entry);
      archive_read_free(archive);

      return found;
    } catch (Exception &ex2) {
      archive_entry_free(entry);
      throw;
    }
  } catch (IOException &ex) {
    archive_read_free(archive);
    return false;
  } catch (Exception &ex) {
    archive_read_free(archive);
    throw;
  }
}

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
