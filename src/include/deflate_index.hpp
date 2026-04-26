#pragma once

#include "duckdb/common/file_system.hpp"
#include <array>
#include <zlib.h>

namespace duckdb {

class DeflateIndex {
public:
  static constexpr idx_t WINDOW_SIZE = 32768;
  static constexpr idx_t CHUNK_SIZE = 16384;

  struct AccessPoint {
    int64_t out = 0;
    int64_t in = 0;
    uint8_t bits = 0;
    uint32_t dict_size = 0;
    std::array<uint8_t, WINDOW_SIZE> window = {};
  };

  static unique_ptr<DeflateIndex> Build(FileHandle &outer_handle,
                                        int64_t compressed_offset,
                                        int64_t compressed_size,
                                        int64_t span = 1048576);

  int64_t Extract(FileHandle &outer_handle, int64_t offset, uint8_t *buf,
                  int64_t len) const;

  int64_t GetUncompressedSize() const { return uncompressed_size; }

private:
  void AddPoint(const z_stream &strm, int64_t absolute_in, int64_t out,
                const std::array<uint8_t, WINDOW_SIZE> &window);

private:
  std::vector<AccessPoint> points;
  int64_t uncompressed_size = 0;
  int64_t compressed_offset = 0;
  int64_t compressed_size = 0;
};

} // namespace duckdb
