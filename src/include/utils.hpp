#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/string_util.hpp"

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

inline idx_t ChunkSize(const DataChunk &chunk) {
  return FlatVector::GetCapacity(chunk.data[0]);
}

// Readers of these formats seek all over the file (footer first, then row
// groups / pages in any order). A compressed archive member cannot be seeked
// without re-inflating from the start, so such members keep the buffered
// (whole member in memory) path; everything else streams in O(block) memory.
inline bool NeedsRandomAccess(const string &member_path) {
  auto lower = StringUtil::Lower(member_path);
  for (auto ext : {".parquet", ".arrow", ".feather", ".ipc", ".xlsx", ".duckdb",
                   ".db", ".sqlite", ".sqlite3"}) {
    if (StringUtil::EndsWith(lower, ext)) {
      return true;
    }
  }
  return false;
}

} // namespace duckdb
