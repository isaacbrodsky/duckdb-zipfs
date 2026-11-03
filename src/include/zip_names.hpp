#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>
#include "utf8proc_wrapper.hpp"

namespace duckdb {

// General purpose bit 11 ("language encoding flag"): the name is UTF-8.
static constexpr mz_uint16 ZIP_FLAG_UTF8_NAME = 1 << 11;

// Code page 437, bytes 0x80-0xFF, as UTF-8. Bytes below 0x80 are ASCII.
// The zip spec (APPNOTE 4.4.4) makes CP437 the encoding of every name that
// does not set bit 11, and that is what Windows' built-in zipper writes.
static const char *const CP437_HIGH[128] = {
    "Ç", "ü", "é", "â", "ä", "à", "å", "ç", "ê", "ë", "è", "ï", "î", "ì", "Ä", "Å",
    "É", "æ", "Æ", "ô", "ö", "ò", "û", "ù", "ÿ", "Ö", "Ü", "¢", "£", "¥", "₧", "ƒ",
    "á", "í", "ó", "ú", "ñ", "Ñ", "ª", "º", "¿", "⌐", "¬", "½", "¼", "¡", "«", "»",
    "░", "▒", "▓", "│", "┤", "╡", "╢", "╖", "╕", "╣", "║", "╗", "╝", "╜", "╛", "┐",
    "└", "┴", "┬", "├", "─", "┼", "╞", "╟", "╚", "╔", "╩", "╦", "╠", "═", "╬", "╧",
    "╨", "╤", "╥", "╙", "╘", "╒", "╓", "╫", "╪", "┘", "┌", "█", "▄", "▌", "▐", "▀",
    "α", "ß", "Γ", "π", "Σ", "σ", "µ", "τ", "Φ", "Θ", "Ω", "δ", "∞", "φ", "ε", "∩",
    "≡", "±", "≥", "≤", "⌠", "⌡", "÷", "≈", "°", "∙", "·", "√", "ⁿ", "²", "■", "\xC2\xA0"};

inline string Cp437ToUtf8(const string &raw) {
  string out;
  out.reserve(raw.size() * 2);
  for (unsigned char c : raw) {
    if (c < 0x80) {
      out += static_cast<char>(c);
    } else {
      out += CP437_HIGH[c - 0x80];
    }
  }
  return out;
}

// The raw (as stored) name of entry `index`.
inline string ZipRawEntryName(mz_zip_archive &zip, mz_uint index) {
  // Includes the terminating NUL; zip names are at most 2**16 - 1 bytes.
  mz_uint size = mz_zip_reader_get_filename(&zip, index, nullptr, 0);
  if (size == 0) {
    throw IOException("Problem getting filename (zero): %s",
                      mz_zip_get_error_string(mz_zip_get_last_error(&zip)));
  }
  string name(size - 1, '\0');
  mz_zip_reader_get_filename(&zip, index, &name[0], size);
  return name;
}

// Info-ZIP Unicode Path extra field (APPNOTE 4.6.9): the UTF-8 name, written by
// WinRAR, 7-Zip and Info-ZIP next to a legacy (code page) name.
static constexpr mz_uint16 ZIP_EXTRA_UNICODE_PATH = 0x7075;
static constexpr idx_t ZIP_CENTRAL_HEADER_SIZE = 46;

static inline mz_uint16 ZipReadU16(const unsigned char *p) {
  return static_cast<mz_uint16>(p[0] | (p[1] << 8));
}

static inline mz_uint32 ZipReadU32(const unsigned char *p) {
  return static_cast<mz_uint32>(p[0]) | (static_cast<mz_uint32>(p[1]) << 8) |
         (static_cast<mz_uint32>(p[2]) << 16) |
         (static_cast<mz_uint32>(p[3]) << 24);
}

// The UTF-8 name in entry `stat`'s Unicode Path extra field, or false when it
// has none, or one that does not describe `raw`: the field carries the CRC-32
// of the legacy name it was written for, and a tool that renamed the entry
// without updating the field leaves a stale one, which must be ignored.
inline bool ZipUnicodePathName(mz_zip_archive &zip,
                               const mz_zip_archive_file_stat &stat,
                               const string &raw, string &result) {
  if (!zip.m_pRead) {
    return false;
  }
  const auto header_ofs =
      zip.m_central_directory_file_ofs + stat.m_central_dir_ofs;
  unsigned char header[ZIP_CENTRAL_HEADER_SIZE];
  if (zip.m_pRead(zip.m_pIO_opaque, header_ofs, header, sizeof(header)) !=
          sizeof(header) ||
      ZipReadU32(header) != 0x02014b50) {
    return false;
  }
  const auto name_len = ZipReadU16(header + 28);
  const auto extra_len = ZipReadU16(header + 30);
  if (extra_len == 0) {
    return false;
  }
  string extra(extra_len, '\0');
  if (zip.m_pRead(zip.m_pIO_opaque,
                  header_ofs + ZIP_CENTRAL_HEADER_SIZE + name_len, &extra[0],
                  extra_len) != extra_len) {
    return false;
  }
  const auto *p = reinterpret_cast<const unsigned char *>(extra.data());
  const auto *end = p + extra.size();
  while (end - p >= 4) {
    const auto id = ZipReadU16(p);
    const auto size = ZipReadU16(p + 2);
    const auto *data = p + 4;
    if (size > end - data) {
      return false;
    }
    // version (1 byte), CRC-32 of the legacy name (4), UTF-8 name
    if (id == ZIP_EXTRA_UNICODE_PATH && size > 5 && data[0] == 1) {
      const auto crc = mz_crc32(
          MZ_CRC32_INIT, reinterpret_cast<const unsigned char *>(raw.data()),
          raw.size());
      const auto *name = reinterpret_cast<const char *>(data + 5);
      const idx_t len = size - 5;
      if (ZipReadU32(data + 1) == crc &&
          Utf8Proc::Analyze(name, len) != UnicodeType::INVALID) {
        result.assign(name, len);
        return true;
      }
      return false;
    }
    p = data + size;
  }
  return false;
}

// The name of entry `index` as UTF-8, which is what DuckDB requires of every
// VARCHAR, so a legacy archive lists (and globs) instead of failing with
// "Invalid unicode (byte sequence mismatch) detected in value construction":
// 1. flagged UTF-8 (bit 11): the name as stored;
// 2. a Unicode Path extra field for this name: the UTF-8 name it holds;
// 3. valid UTF-8 anyway (many tools write UTF-8 without the flag): as stored;
// 4. otherwise CP437, the encoding APPNOTE gives unflagged names.
inline string ZipEntryName(mz_zip_archive &zip, mz_uint index) {
  auto raw = ZipRawEntryName(zip, index);
  // Stat only to read the flag and the entry's offset: it records errors of
  // its own (an encrypted entry is "unsupported encryption") that callers would
  // take for a failure to read the name, so leave the error state as the name
  // read left it.
  const auto name_error = mz_zip_peek_last_error(&zip);
  mz_zip_archive_file_stat stat;
  const auto have_stat = mz_zip_reader_file_stat(&zip, index, &stat);
  mz_zip_set_last_error(&zip, name_error);
  if (have_stat && (stat.m_bit_flag & ZIP_FLAG_UTF8_NAME)) {
    return raw;
  }
  string unicode_path;
  if (have_stat && ZipUnicodePathName(zip, stat, raw, unicode_path)) {
    return unicode_path;
  }
  if (Utf8Proc::Analyze(raw.c_str(), raw.size()) != UnicodeType::INVALID) {
    return raw;
  }
  return Cp437ToUtf8(raw);
}

// Find an entry by the name ZipEntryName gives it. Tries miniz's lookup on the
// stored bytes first, then falls back to comparing decoded names, so a path
// taken from glob() or zip_contents() opens even when the stored name is CP437.
// Case-insensitive, like miniz's lookup with no flags.
inline bool ZipLocateEntry(mz_zip_archive &zip, const string &name,
                           mz_uint &index) {
  if (mz_zip_reader_locate_file_v2(&zip, name.c_str(), nullptr, 0, &index)) {
    return true;
  }
  mz_zip_clear_last_error(&zip);
  auto count = mz_zip_reader_get_num_files(&zip);
  for (mz_uint i = 0; i < count; i++) {
    if (StringUtil::CIEquals(ZipEntryName(zip, i), name)) {
      index = i;
      return true;
    }
  }
  return false;
}

} // namespace duckdb
