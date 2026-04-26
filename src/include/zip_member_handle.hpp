#pragma once

#include "deflate_index.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>

namespace duckdb {

class ZipMemberHandle : public FileHandle {
public:
  ZipMemberHandle(FileSystem &file_system, const string &path,
                  FileOpenFlags flags, unique_ptr<FileHandle> outer_handle_p,
                  const mz_zip_archive_file_stat &file_stat_p)
      : FileHandle(file_system, path, flags),
        outer_handle(std::move(outer_handle_p)), file_stat(file_stat_p),
        seek_offset(0), closed(false) {}

  ~ZipMemberHandle() override;
  void Close() override;

  int64_t Read(void *buffer, int64_t nr_bytes);
  int64_t ReadAt(void *buffer, int64_t nr_bytes, idx_t location);
  void Seek(idx_t location);
  void Reset();
  idx_t SeekPosition() const;
  int64_t GetFileSize() const;
  timestamp_t GetLastModifiedTime() const;
  FileType GetFileType() const;

protected:
  virtual int64_t ReadInternal(void *buffer, int64_t nr_bytes,
                               idx_t location) = 0;
  virtual idx_t GetMemberSize() const = 0;

protected:
  unique_ptr<FileHandle> outer_handle;
  mz_zip_archive_file_stat file_stat;
  idx_t seek_offset;

private:
  bool closed;
};

class StoredMemberHandle final : public ZipMemberHandle {
public:
  StoredMemberHandle(FileSystem &file_system, const string &path,
                     FileOpenFlags flags, unique_ptr<FileHandle> outer_handle_p,
                     const mz_zip_archive_file_stat &file_stat_p,
                     int64_t data_offset_p)
      : ZipMemberHandle(file_system, path, flags, std::move(outer_handle_p),
                        file_stat_p),
        data_offset(data_offset_p),
        member_size(UnsafeNumericCast<idx_t>(file_stat_p.m_uncomp_size)) {}

protected:
  int64_t ReadInternal(void *buffer, int64_t nr_bytes, idx_t location) override;
  idx_t GetMemberSize() const override { return member_size; }

private:
  int64_t data_offset;
  idx_t member_size;
};

class BufferedMemberHandle final : public ZipMemberHandle {
public:
  BufferedMemberHandle(FileSystem &file_system, const string &path,
                       FileOpenFlags flags,
                       unique_ptr<FileHandle> outer_handle_p,
                       const mz_zip_archive_file_stat &file_stat_p,
                       vector<data_t> data_p)
      : ZipMemberHandle(file_system, path, flags, std::move(outer_handle_p),
                        file_stat_p),
        data(std::move(data_p)) {}

protected:
  int64_t ReadInternal(void *buffer, int64_t nr_bytes, idx_t location) override;
  idx_t GetMemberSize() const override {
    return UnsafeNumericCast<idx_t>(data.size());
  }

private:
  vector<data_t> data;
};

class ZranMemberHandle final : public ZipMemberHandle {
public:
  ZranMemberHandle(FileSystem &file_system, const string &path,
                   FileOpenFlags flags, unique_ptr<FileHandle> outer_handle_p,
                   const mz_zip_archive_file_stat &file_stat_p,
                   unique_ptr<DeflateIndex> index_p)
      : ZipMemberHandle(file_system, path, flags, std::move(outer_handle_p),
                        file_stat_p),
        index(std::move(index_p)) {}

protected:
  int64_t ReadInternal(void *buffer, int64_t nr_bytes, idx_t location) override;
  idx_t GetMemberSize() const override {
    return UnsafeNumericCast<idx_t>(index->GetUncompressedSize());
  }

private:
  unique_ptr<DeflateIndex> index;
};

} // namespace duckdb
