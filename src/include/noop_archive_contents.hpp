#pragma once

#ifndef ENABLE_LIBARCHIVE

#include "utils.hpp"

namespace duckdb {

void NoopReadArchiveFunction(ClientContext &context, TableFunctionInput &data,
                             DataChunk &output);

unique_ptr<FunctionData> NoopReadArchiveFunctionBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState>
NoopReadArchiveFunctionInit(ClientContext &context,
                            TableFunctionInitInput &input);

} // namespace duckdb

#endif // ENABLE_LIBARCHIVE
