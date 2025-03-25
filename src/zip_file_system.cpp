#include "zip_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"

namespace duckdb {

// TODO: Something is incorrect about the type in make_uniq_array<...,
// std::default_delete<DATA_TYPE>, ...>
template <class DATA_TYPE>
inline unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>
make_uniq_array2(size_t n) // NOLINT: mimic std style
{
  return unique_ptr<DATA_TYPE[], std::default_delete<DATA_TYPE[]>, true>(
      new DATA_TYPE[n]());
}

//------------------------------------------------------------------------------
// Zip Utilities
//------------------------------------------------------------------------------

// Split a tar path into the path to the archive and the path within the archive
static pair<string, string> SplitArchivePath(const string &path) {
  // TODO: use some escaping here
  const string suffix = ".zip";

  const auto zip_path =
      std::search(path.begin(), path.end(), suffix.begin(), suffix.end());

  if (zip_path == path.end()) {
    throw IOException("Could not find a '.zip' archive to open in: '%s'", path);
  }

  const auto suffix_path = zip_path + UnsafeNumericCast<int64_t>(suffix.size());
  if (suffix_path == path.end()) {
    return {path, ""};
  }

  if (*suffix_path == '/') {
    // If there is a slash after the last .zip, we need to remove everything
    // after that
    auto archive_path = string(path.begin(), suffix_path);
    auto file_path = string(suffix_path + 1, path.end());
    return {archive_path, file_path};
  }

  throw IOException("Could not find a '.zip' archive to open in: '%s'.", path);
}

//------------------------------------------------------------------------------
// Zip File Handle
//------------------------------------------------------------------------------

void ZipFileHandle::Close() { inner_handle->Close(); }

//------------------------------------------------------------------------------
// Zip File System
//------------------------------------------------------------------------------

bool ZipFileSystem::CanHandleFile(const string &fpath) {
  // TODO: Check that we can seek into the file
  return fpath.size() > 6 && fpath.substr(0, 6) == "zip://";
}

size_t FileSystemZipReadFunc(void *pOpaque, mz_uint64 file_ofs, void *pBuf,
                             size_t n) {
  FileHandle *handle = (FileHandle *)pOpaque;
  handle->Seek(UnsafeNumericCast<idx_t>(file_ofs));
  return UnsafeNumericCast<size_t>(handle->Read(pBuf, n));
}

unique_ptr<FileHandle>
ZipFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                        optional_ptr<FileOpener> opener) {
  if (!flags.OpenForReading() || flags.OpenForWriting()) {
    throw IOException("Zip file system can only open for reading");
  }

  // Get the path to the zip file
  const auto paths = SplitArchivePath(path.substr(6));
  const auto &zip_path = paths.first;
  const auto &file_path = paths.second;

  // Now we need to find the file within the zip file and return out file handle
  auto &fs = FileSystem::GetFileSystem(*opener->TryGetClientContext());
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

  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = handle.get();
  try {
    mz_uint zip_flags = 0;

    if (!mz_zip_reader_init(&zip, size, zip_flags)) {
      throw IOException("Failed to init miniz");
    }

    mz_uint file_index = 0;
    auto locate_failed =
        mz_zip_reader_locate_file_v2(&zip, file_path.c_str(), nullptr, 0,
                                     &file_index) == MZ_FALSE;
    if (locate_failed) {
      throw IOException("Failed to find file: %s", file_path);
    }

    mz_zip_archive_file_stat file_stat = {0};
    auto stat_failed =
        mz_zip_reader_file_stat(&zip, file_index, &file_stat) == MZ_FALSE;

    if (stat_failed) {
      throw IOException("Problem stat-ing file within archive");
    }
    if ((file_stat.m_method) && (file_stat.m_method != MZ_DEFLATED)) {
      throw IOException("Unknown compression method");
    }

    auto read_buf = make_uniq_array2<data_t>(file_stat.m_uncomp_size);
    mz_zip_reader_extract_file_to_mem(
        &zip, file_stat.m_filename, read_buf.get(), file_stat.m_uncomp_size, 0);

    auto zip_file_handle = make_uniq<ZipFileHandle>(
        *this, path, flags, std::move(handle), file_stat, std::move(read_buf));

    mz_zip_reader_end(&zip);

    return zip_file_handle;
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }
}

int64_t ZipFileSystem::Read(FileHandle &handle, void *buffer,
                            int64_t nr_bytes) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  auto position = t_handle.seek_offset;
  auto remaining_bytes = t_handle.file_stat.m_uncomp_size - position;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  memcpy(buffer, t_handle.data.get() + position, to_read);
  t_handle.seek_offset += to_read;
  return to_read;
}

int64_t ZipFileSystem::GetFileSize(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return UnsafeNumericCast<int64_t>(t_handle.file_stat.m_uncomp_size);
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
  auto &inner_handle = *t_handle.inner_handle;
  return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType ZipFileSystem::GetFileType(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  auto &inner_handle = *t_handle.inner_handle;
  return inner_handle.file_system.GetFileType(inner_handle);
}

bool ZipFileSystem::OnDiskFile(FileHandle &handle) {
  auto &t_handle = handle.Cast<ZipFileHandle>();
  return t_handle.inner_handle->OnDiskFile();
}

vector<string> ZipFileSystem::Glob(const string &path, FileOpener *opener) {
  // Remove the "zip://" prefix
  const auto parts = SplitArchivePath(path.substr(6));
  auto &zip_path = parts.first;
  auto &file_path = parts.second;

  if (HasGlob(zip_path)) {
    throw NotImplementedException("Cannot glob multiple zip files");
  }

  if (!HasGlob(file_path)) {
    // No glob pattern in the file path, just return the file path
    return {path};
  }

  auto pattern_parts = StringUtil::Split(file_path, '/');
  for (auto &part : pattern_parts) {
    if (part == "zip:" || StringUtil::EndsWith(part, ".zip")) {
      // We can not glob into nested zip files
      throw NotImplementedException(
          "Globbing into nested zip files is not supported");
    }
  }

  optional_ptr<ClientContext> context = opener->TryGetClientContext();

  // Given the path to the zip file, open it
  auto &fs = FileSystem::GetFileSystem(*context);

  auto archive_handle = fs.OpenFile(zip_path, FileFlags::FILE_FLAGS_READ);
  if (!archive_handle) {
    throw IOException("Failed to open file: %s", zip_path);
  }
  if (!archive_handle->CanSeek()) {
    // TODO: Buffer?
    throw IOException("Cannot seek");
  }

  idx_t size = archive_handle->GetFileSize();

  vector<string> result;
  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  zip.m_pRead = &FileSystemZipReadFunc;
  zip.m_pIO_opaque = archive_handle.get();
  const size_t MAX_FILENAME_SIZE = 512;
  char filename_buffer[MAX_FILENAME_SIZE];
  try {
    mz_uint flags = 0;

    if (!mz_zip_reader_init(&zip, size, flags)) {
      throw IOException("Failed to init miniz");
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

      mz_uint filename_size =
          mz_zip_reader_get_filename(&zip, i, nullptr, 0) + 1;
      if (filename_size > MAX_FILENAME_SIZE) {
        // TODO: Fix properly
        throw IOException("Encountered very long filename");
      }
      mz_zip_reader_get_filename(&zip, i, filename_buffer, filename_size);

      if (mz_zip_get_last_error(&zip)) {
        throw IOException("Problem getting filename");
      }

      auto entry_parts = StringUtil::Split(std::string(filename_buffer), '/');

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
        auto entry_path = "zip://" + zip_path + "/" + filename_buffer;
        // Cache here???
        result.push_back(entry_path);
      }
    }

    mz_zip_reader_end(&zip);
  } catch (Exception &ex) {
    mz_zip_reader_end(&zip);
    throw;
  }

  return result;
}

} // namespace duckdb