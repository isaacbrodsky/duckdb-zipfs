#include "zip_member_handle.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include <cstring>

namespace duckdb {

ZipMemberHandle::~ZipMemberHandle() {
  try {
    Close();
  } catch (...) { // NOLINT
  }
}

void ZipMemberHandle::Close() {
  if (closed) {
    return;
  }
  closed = true;

  if (outer_handle) {
    outer_handle->Close();
  }
}

int64_t ZipMemberHandle::Read(void *buffer, int64_t nr_bytes) {
  auto read_bytes = ReadAt(buffer, nr_bytes, seek_offset);
  seek_offset += UnsafeNumericCast<idx_t>(read_bytes);
  return read_bytes;
}

int64_t ZipMemberHandle::ReadAt(void *buffer, int64_t nr_bytes,
                                idx_t location) {
  if (nr_bytes <= 0) {
    return 0;
  }
  if (location >= GetMemberSize()) {
    return 0;
  }
  return ReadInternal(buffer, nr_bytes, location);
}

void ZipMemberHandle::Seek(idx_t location) {
  seek_offset = MinValue(location, GetMemberSize());
}

void ZipMemberHandle::Reset() { seek_offset = 0; }

idx_t ZipMemberHandle::SeekPosition() const { return seek_offset; }

int64_t ZipMemberHandle::GetFileSize() const {
  return UnsafeNumericCast<int64_t>(GetMemberSize());
}

timestamp_t ZipMemberHandle::GetLastModifiedTime() const {
  auto &inner_handle = *outer_handle;
  return inner_handle.file_system.GetLastModifiedTime(inner_handle);
}

FileType ZipMemberHandle::GetFileType() const {
  auto &inner_handle = *outer_handle;
  return inner_handle.file_system.GetFileType(inner_handle);
}

int64_t StoredMemberHandle::ReadInternal(void *buffer, int64_t nr_bytes,
                                         idx_t location) {
  auto remaining_bytes = member_size - location;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  outer_handle->Read(buffer, to_read,
                     UnsafeNumericCast<idx_t>(data_offset) + location);
  return UnsafeNumericCast<int64_t>(to_read);
}

int64_t BufferedMemberHandle::ReadInternal(void *buffer, int64_t nr_bytes,
                                           idx_t location) {
  auto remaining_bytes = GetMemberSize() - location;
  auto to_read = MinValue(UnsafeNumericCast<idx_t>(nr_bytes), remaining_bytes);
  memcpy(buffer, data.data() + location, to_read);
  return UnsafeNumericCast<int64_t>(to_read);
}

int64_t ZranMemberHandle::ReadInternal(void *buffer, int64_t nr_bytes,
                                       idx_t location) {
  return index->Extract(*outer_handle, UnsafeNumericCast<int64_t>(location),
                        reinterpret_cast<uint8_t *>(buffer), nr_bytes);
}

} // namespace duckdb
