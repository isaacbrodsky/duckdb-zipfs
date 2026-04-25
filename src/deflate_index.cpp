#include "deflate_index.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include <algorithm>
#include <cstring>
#include <limits>

namespace duckdb {

namespace {

static constexpr int ZIP_RAW_DEFLATE_WINDOW_BITS = -15;

static string ZlibErrorMessage(int ret) {
  switch (ret) {
  case Z_MEM_ERROR:
    return "out of memory";
  case Z_BUF_ERROR:
    return "compressed member ended prematurely";
  case Z_DATA_ERROR:
    return "compressed member is corrupt";
  case Z_STREAM_ERROR:
    return "invalid zlib stream state";
  default:
    return "zlib error " + std::to_string(ret);
  }
}

static void ThrowZlibError(const char *operation, int ret) {
  throw IOException("%s failed: %s", operation, ZlibErrorMessage(ret).c_str());
}

} // namespace

void DeflateIndex::AddPoint(const z_stream &strm, int64_t absolute_in,
                            int64_t out,
                            const std::array<uint8_t, WINDOW_SIZE> &window) {
  AccessPoint point;
  point.out = out;
  point.in = absolute_in;
  point.bits = UnsafeNumericCast<uint8_t>(strm.data_type & 7);
  point.dict_size = UnsafeNumericCast<uint32_t>(
      MinValue<int64_t>(WINDOW_SIZE, MaxValue<int64_t>(out, 0)));

  if (point.dict_size > 0) {
    auto recent = UnsafeNumericCast<uint32_t>(WINDOW_SIZE - strm.avail_out);
    auto copy = MinValue(recent, point.dict_size);
    if (copy > 0) {
      memcpy(point.window.data() + point.dict_size - copy,
             window.data() + recent - copy, copy);
    }
    auto remaining = point.dict_size - copy;
    if (remaining > 0) {
      memcpy(point.window.data(), window.data() + WINDOW_SIZE - remaining,
             remaining);
    }
  }

  points.push_back(point);
}

unique_ptr<DeflateIndex> DeflateIndex::Build(FileHandle &outer_handle,
                                             int64_t compressed_offset,
                                             int64_t compressed_size,
                                             int64_t span) {
  auto index = make_uniq<DeflateIndex>();
  index->compressed_offset = compressed_offset;
  index->compressed_size = compressed_size;

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  auto ret = inflateInit2(&strm, ZIP_RAW_DEFLATE_WINDOW_BITS);
  if (ret != Z_OK) {
    ThrowZlibError("inflateInit2", ret);
  }

  try {
    std::array<uint8_t, CHUNK_SIZE> input = {};
    std::array<uint8_t, WINDOW_SIZE> window = {};
    int64_t read_offset = compressed_offset;
    int64_t totin = 0;
    int64_t totout = 0;
    int64_t last = 0;

    index->points.reserve(UnsafeNumericCast<size_t>(
                              MaxValue<int64_t>(1, compressed_size / span)) +
                          1);
    index->points.push_back(AccessPoint{});
    index->points.back().out = 0;
    index->points.back().in = compressed_offset;

    if (compressed_size == 0) {
      index->uncompressed_size = 0;
      inflateEnd(&strm);
      return index;
    }

    do {
      if (strm.avail_in == 0 &&
          read_offset < compressed_offset + compressed_size) {
        auto to_read = MinValue<int64_t>(
            UnsafeNumericCast<int64_t>(input.size()),
            compressed_offset + compressed_size - read_offset);
        outer_handle.Read(input.data(), UnsafeNumericCast<idx_t>(to_read),
                          UnsafeNumericCast<idx_t>(read_offset));
        read_offset += to_read;
        totin += to_read;
        strm.avail_in = UnsafeNumericCast<uInt>(to_read);
        strm.next_in = input.data();
      }

      if (strm.avail_out == 0) {
        strm.avail_out = UnsafeNumericCast<uInt>(window.size());
        strm.next_out = window.data();
      }

      auto before = strm.avail_out;
      ret = inflate(&strm, Z_BLOCK);
      totout += before - strm.avail_out;

      if ((strm.data_type & 0xc0) == 0x80 && totout - last >= span) {
        index->AddPoint(strm, compressed_offset + totin - strm.avail_in, totout,
                        window);
        last = totout;
      }
    } while (ret == Z_OK);

    if (ret != Z_STREAM_END) {
      ThrowZlibError("inflate(Z_BLOCK)",
                     ret == Z_NEED_DICT ? Z_DATA_ERROR : ret);
    }

    index->uncompressed_size = totout;
    inflateEnd(&strm);
    return index;
  } catch (...) {
    inflateEnd(&strm);
    throw;
  }
}

int64_t DeflateIndex::Extract(FileHandle &outer_handle, int64_t offset,
                              uint8_t *buf, int64_t len) const {
  if (len <= 0 || offset < 0 || offset >= uncompressed_size) {
    return 0;
  }

  auto requested = MinValue<int64_t>(len, uncompressed_size - offset);
  auto point_it =
      std::upper_bound(points.begin(), points.end(), offset,
                       [](int64_t target, const AccessPoint &point) {
                         return target < point.out;
                       });
  if (point_it == points.begin()) {
    throw IOException("Invalid deflate index state");
  }
  --point_it;
  auto &point = *point_it;

  z_stream strm;
  memset(&strm, 0, sizeof(strm));
  auto ret = inflateInit2(&strm, ZIP_RAW_DEFLATE_WINDOW_BITS);
  if (ret != Z_OK) {
    ThrowZlibError("inflateInit2", ret);
  }

  try {
    std::array<uint8_t, CHUNK_SIZE> input = {};
    std::array<uint8_t, WINDOW_SIZE> discard = {};
    int64_t read_offset = point.in - (point.bits ? 1 : 0);
    int64_t skip = offset - point.out;
    int64_t left = requested;

    if (point.bits) {
      uint8_t ch;
      outer_handle.Read(&ch, 1, UnsafeNumericCast<idx_t>(read_offset));
      read_offset++;
      ret = inflatePrime(&strm, point.bits, ch >> (8 - point.bits));
      if (ret != Z_OK) {
        ThrowZlibError("inflatePrime", ret);
      }
    }

    if (point.dict_size > 0) {
      ret = inflateSetDictionary(&strm, point.window.data(), point.dict_size);
      if (ret != Z_OK) {
        ThrowZlibError("inflateSetDictionary", ret);
      }
    }

    do {
      if (skip > 0) {
        auto out_size =
            MinValue<int64_t>(skip, UnsafeNumericCast<int64_t>(discard.size()));
        strm.avail_out = UnsafeNumericCast<uInt>(out_size);
        strm.next_out = discard.data();
      } else {
        auto out_size = MinValue<int64_t>(
            left, UnsafeNumericCast<int64_t>(std::numeric_limits<uInt>::max()));
        strm.avail_out = UnsafeNumericCast<uInt>(out_size);
        strm.next_out = buf + (requested - left);
      }

      if (strm.avail_in == 0) {
        if (read_offset >= compressed_offset + compressed_size) {
          ThrowZlibError("inflate", Z_BUF_ERROR);
        }
        auto to_read = MinValue<int64_t>(
            UnsafeNumericCast<int64_t>(input.size()),
            compressed_offset + compressed_size - read_offset);
        outer_handle.Read(input.data(), UnsafeNumericCast<idx_t>(to_read),
                          UnsafeNumericCast<idx_t>(read_offset));
        read_offset += to_read;
        strm.avail_in = UnsafeNumericCast<uInt>(to_read);
        strm.next_in = input.data();
      }

      auto before = strm.avail_out;
      ret = inflate(&strm, Z_NO_FLUSH);
      auto got = before - strm.avail_out;

      if (skip > 0) {
        skip -= got;
      } else {
        left -= got;
        if (left == 0) {
          break;
        }
      }

      if (ret == Z_STREAM_END) {
        break;
      }
      if (ret != Z_OK) {
        ThrowZlibError("inflate", ret == Z_NEED_DICT ? Z_DATA_ERROR : ret);
      }
    } while (true);

    inflateEnd(&strm);
    return requested - left;
  } catch (...) {
    inflateEnd(&strm);
    throw;
  }
}

} // namespace duckdb
