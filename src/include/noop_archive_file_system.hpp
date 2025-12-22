#pragma once

#ifndef ENABLE_LIBARCHIVE

#include "duckdb/common/file_system.hpp"

namespace duckdb {

class NoopArchiveFileSystem final : public FileSystem {
public:
  explicit NoopArchiveFileSystem() : FileSystem() {}

  std::string GetName() const override { return "NoopArchiveFileSystem"; }

  bool CanHandleFile(const string &fpath) override;
  bool CanSeek() override { return true; }

  unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
                                  optional_ptr<FileOpener> opener) override;

private:
};

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
