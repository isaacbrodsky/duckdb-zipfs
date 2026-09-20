#include "zipfs_secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

static bool CheckValidPassword(CreateSecretInput &input) {
  auto password = input.options.find("password");
  if (password != input.options.end()) {
    auto value = password->second;
    if (value.IsNull()) {
      throw InvalidInputException("password cannot be NULL");
    }
    if (value.GetValue<string>() == "") {
      throw InvalidInputException("password cannot be empty");
    }
    return true;
  }
  return false;
}

static bool CheckValidPasswords(CreateSecretInput &input) {
  auto passwords = input.options.find("passwords");
  if (passwords != input.options.end()) {
    auto value = passwords->second;
    if (value.IsNull()) {
      throw InvalidInputException("passwords cannot be NULL");
    }
    auto children = ListValue::GetChildren(value);
    if (children.size() == 0) {
      throw InvalidInputException("passwords cannot be empty");
    }
    for (const auto &child : children) {
      if (child.IsNull() || child.GetValue<string>() == "") {
        throw InvalidInputException("passwords cannot contain empty or NULL");
      }
    }
    return true;
  }
  return false;
}

unique_ptr<BaseSecret> CreateZipSecretFunction(ClientContext &context,
                                               CreateSecretInput &input) {
  auto scope = input.scope;
  if (scope.empty()) {
    scope = {"archive://"};
  }
  auto secret =
      make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
  bool validPassword = CheckValidPassword(input);
  bool validPasswords = CheckValidPasswords(input);
  if (!validPassword && !validPasswords) {
    throw InvalidInputException("need to set password or passwords list");
  }
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
