#include "archive_contents.hpp"
#include "archive_file_system.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"

#ifndef ENABLE_LIBARCHIVE

namespace duckdb {

void NoopReadArchiveFunction(ClientContext &context, TableFunctionInput &data,
                             DataChunk &output) {
  throw NotImplementedException("duckdb-zipfs was not built with libarchive "
                                "support. (Not supported on Windows)");
}

unique_ptr<FunctionData> NoopReadArchiveFunctionBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  return_types.push_back(LogicalType::VARCHAR);
  names.emplace_back("file_name");

  return_types.push_back(LogicalType::UBIGINT);
  names.emplace_back("file_size");

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("is_directory");

  return nullptr;
}

unique_ptr<GlobalTableFunctionState>
NoopReadArchiveFunctionInit(ClientContext &context,
                            TableFunctionInitInput &input) {
  return nullptr;
}

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
