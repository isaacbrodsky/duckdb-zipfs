#pragma once

#include <miniz/miniz.h>
#include <miniz/miniz_zip.h>
#include "utils.hpp"

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
