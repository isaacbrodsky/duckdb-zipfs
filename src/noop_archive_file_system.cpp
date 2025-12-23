#include "noop_archive_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

#ifndef ENABLE_LIBARCHIVE

namespace duckdb {

bool NoopArchiveFileSystem::CanHandleFile(const string &fpath) {
  auto isArchive = fpath.size() > 10 && fpath.substr(0, 10) == "archive://";
  if (isArchive) {
    throw NotImplementedException("duckdb-zip was not built with libarchive "
                                  "support. (Not supported on Windows)");
  }
  return false;
}

unique_ptr<FileHandle>
NoopArchiveFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                optional_ptr<FileOpener> opener) {
  throw NotImplementedException("duckdb-zip was not built with libarchive "
                                "support. (Not supported on Windows)");
}

bool NoopRawArchiveFileSystem::CanHandleFile(const string &fpath) {
  auto isArchive = fpath.size() > 13 && fpath.substr(0, 13) == "compressed://";
  if (isArchive) {
    throw NotImplementedException("duckdb-zip was not built with libarchive "
                                  "support. (Not supported on Windows)");
  }
  return false;
}

unique_ptr<FileHandle>
NoopRawArchiveFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                   optional_ptr<FileOpener> opener) {
  throw NotImplementedException("duckdb-zip was not built with libarchive "
                                "support. (Not supported on Windows)");
}

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
