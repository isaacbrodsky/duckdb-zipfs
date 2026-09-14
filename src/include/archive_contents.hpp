#pragma once

#ifdef ENABLE_LIBARCHIVE

#include "duckdb.hpp"
#include <archive.h>
#include <archive_entry.h>
#include "utils.hpp"

namespace duckdb {

void ReadArchiveFunction(ClientContext &context, TableFunctionInput &data,
                         DataChunk &output);

unique_ptr<FunctionData>
ReadArchiveFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<Identifier> &names);

unique_ptr<GlobalTableFunctionState>
ReadArchiveFunctionInit(ClientContext &context, TableFunctionInitInput &input);

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
