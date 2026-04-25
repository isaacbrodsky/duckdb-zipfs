#pragma once

#include "utils.hpp"
#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>

namespace duckdb {

void ReadZipFunction(ClientContext &context, TableFunctionInput &data,
                     DataChunk &output);

unique_ptr<FunctionData> ReadZipFunctionBind(ClientContext &context,
                                             TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
                                             vector<string> &names);

unique_ptr<GlobalTableFunctionState>
ReadZipFunctionInit(ClientContext &context, TableFunctionInitInput &input);

} // namespace duckdb
