#pragma once

#include "duckdb.hpp"

#ifdef ENABLE_LIBARCHIVE

#include <archive.h>

#endif

namespace duckdb {

unique_ptr<BaseSecret> CreateZipSecretFunction(ClientContext &context,
                                               CreateSecretInput &input);

#ifdef ENABLE_LIBARCHIVE
void RegisterPassphrasesToArchive(ClientContext &context,
                                  const string &archivePath,
                                  struct archive *archive);
#endif

} // namespace duckdb
