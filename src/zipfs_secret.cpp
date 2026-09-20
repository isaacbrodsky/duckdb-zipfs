#include "zipfs_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

unique_ptr<BaseSecret> CreateZipSecretFunction(ClientContext &context,
                                               CreateSecretInput &input) {
  auto scope = input.scope;
  if (scope.empty()) {
    scope = {"zip://", "archive://", "compressed://", ""};
  }
  auto secret =
      make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
  secret->TrySetValue("password", input);
  secret->TrySetValue("passwords", input);
  secret->redact_keys.insert("password");
  secret->redact_keys.insert("passwords");
  return std::move(secret);
}

#ifdef ENABLE_LIBARCHIVE
void RegisterPassphrasesToArchive(ClientContext &context,
                                  const string &archivePath,
                                  struct archive *archive) {
  auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
  auto match =
      SecretManager::Get(context).LookupSecret(transaction, archivePath, "zip");
  if (match.HasMatch()) {
    if (auto kv = dynamic_cast<const KeyValueSecret *>(&match.GetSecret())) {
      Value value;
      if (kv->TryGetValue("password", value) && !value.IsNull()) {
        if (archive_read_add_passphrase(archive,
                                        value.GetValue<string>().c_str())) {
          throw IOException("Failed to register archive passphrase: %s",
                            archive_error_string(archive));
        }
      }

      Value listValue;
      if (kv->TryGetValue("passwords", listValue) && !listValue.IsNull()) {
        for (const auto &child : ListValue::GetChildren(listValue)) {
          if (!child.IsNull()) {
            if (archive_read_add_passphrase(archive,
                                            child.GetValue<string>().c_str())) {
              throw IOException("Failed to register archive passphrase: %s",
                                archive_error_string(archive));
            }
          }
        }
      }
    }
  }
}
#endif

} // namespace duckdb
