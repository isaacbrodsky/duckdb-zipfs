#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"

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

} // namespace duckdb
