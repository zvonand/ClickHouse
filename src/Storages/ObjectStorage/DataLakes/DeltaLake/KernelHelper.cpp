#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/S3/Configuration.h>
#include <Storages/ObjectStorage/Local/Configuration.h>
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Common/logger_useful.h>

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/identity/client_secret_credential.hpp>
#include <azure/identity/workload_identity_credential.hpp>
#include <azure/identity/managed_identity_credential.hpp>
#endif


namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DB::S3AuthSetting
{
    extern const S3AuthSettingsBool no_sign_request;
}

namespace DeltaLake
{

/// A helper class to manage S3-compatible storage types.
class S3KernelHelper final : public IKernelHelper
{
public:
    S3KernelHelper(
        const DB::S3::URI & url_,
        std::shared_ptr<const DB::S3::Client> client_,
        const DB::S3::S3AuthSettings & auth_settings)
        : url(url_)
        , table_location(getTableLocation(url_))
        , client(client_)
    {
        region = client->getRegion();
        if (region.empty() || region == Aws::Region::AWS_GLOBAL)
            region = client->getRegionForBucket(url.bucket, /* force_detect */true);

        /// Check if user didn't mention any region.
        /// Same as in S3/Client.cpp (stripping len("https://s3.")).
        if (url.endpoint.substr(11) == "amazonaws.com")
            url.addRegionToURI(region);

        no_sign = auth_settings[DB::S3AuthSetting::no_sign_request];
    }

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return url.key; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        auto set_option = [&](const std::string & name, const std::string & value)
        {
            ffi::set_builder_option(builder, KernelUtils::toDeltaString(name), KernelUtils::toDeltaString(value));
        };

        const auto & credentials = client->getCredentials();
        auto access_key_id = credentials.GetAWSAccessKeyId();
        auto secret_access_key = credentials.GetAWSSecretKey();
        auto token = credentials.GetSessionToken();

        /// Supported options
        /// https://github.com/apache/arrow-rs-object-store/blob/main/src/aws/builder.rs#L446
        if (!access_key_id.empty())
            set_option("aws_access_key_id", access_key_id);
        if (!secret_access_key.empty())
            set_option("aws_secret_access_key", secret_access_key);

        /// Set even if token is empty to prevent delta-kernel
        /// from trying to access token api.
        set_option("aws_token", token);

        if (no_sign || (access_key_id.empty() && secret_access_key.empty()))
            set_option("aws_skip_signature", "true");

        if (!region.empty())
            set_option("aws_region", region);

        set_option("aws_bucket", url.bucket);

        if (url.uri_str.starts_with("http"))
        {
            set_option("allow_http", "true");
            set_option("aws_endpoint", url.endpoint);
        }

        LOG_TRACE(
            log,
            "Using endpoint: {}, uri: {}, region: {}, bucket: {}, no sign: {}, "
            "has access_key_id: {}, has secret_access_key: {}, has token: {}",
            url.endpoint, url.uri_str, region, url.bucket, no_sign,
            !access_key_id.empty(), !secret_access_key.empty(), !token.empty());

        return builder;
    }

private:
    DB::S3::URI url;
    const std::string table_location;
    const std::shared_ptr<const DB::S3::Client> client;
    const LoggerPtr log = getLogger("S3KernelHelper");

    std::string region;
    bool no_sign;

    static std::string getTableLocation(const DB::S3::URI & url)
    {
        return "s3://" + url.bucket + "/" + url.key;
    }
};

#if USE_AZURE_BLOB_STORAGE
/// A helper class to manage Azure Blob Storage.
class AzureKernelHelper final : public IKernelHelper
{
public:
    AzureKernelHelper(
        const DB::AzureBlobStorage::ConnectionParams & connection_params_,
        const std::string & blob_path_)
        : connection_params(connection_params_)
        , table_location(buildTableLocation(connection_params_, blob_path_))
        , data_path(blob_path_)
    {}

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return data_path; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        auto set_option = [&](const std::string & name, const std::string & value)
        {
            ffi::set_builder_option(builder, KernelUtils::toDeltaString(name), KernelUtils::toDeltaString(value));
        };

        const auto & endpoint = connection_params.endpoint;

        set_option("azure_container_name", endpoint.container_name);

        std::visit([&](const auto & auth)
        {
            using T = std::decay_t<decltype(auth)>;
            if constexpr (std::is_same_v<T, DB::AzureBlobStorage::ConnectionString>)
            {
                set_option("azure_storage_connection_string", auth.toUnderType());
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Azure::Storage::StorageSharedKeyCredential>>)
            {
                set_option("azure_storage_account_name", auth->AccountName);
                if (connection_params.raw_account_key.has_value())
                    set_option("azure_storage_account_key", *connection_params.raw_account_key);
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Azure::Identity::ClientSecretCredential>>)
            {
                if (connection_params.raw_client_id.has_value())
                    set_option("azure_client_id", *connection_params.raw_client_id);
                if (connection_params.raw_client_secret.has_value())
                    set_option("azure_client_secret", *connection_params.raw_client_secret);
                if (connection_params.raw_tenant_id.has_value())
                    set_option("azure_tenant_id", *connection_params.raw_tenant_id);
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Azure::Identity::WorkloadIdentityCredential>>)
            {
                set_option("azure_use_workload_identity", "true");
                if (!endpoint.account_name.empty())
                    set_option("azure_storage_account_name", endpoint.account_name);
            }
            else if constexpr (std::is_same_v<T, std::shared_ptr<Azure::Identity::ManagedIdentityCredential>>)
            {
                if (!endpoint.account_name.empty())
                    set_option("azure_storage_account_name", endpoint.account_name);
            }
            // StaticCredential and other variants are not supported by delta-kernel-rs;
            // no options are set and the builder will use default credential discovery.
        }, connection_params.auth_method);

        if (!endpoint.sas_auth.empty())
            set_option("azure_storage_sas_key", endpoint.sas_auth);

        /// For non-standard endpoints (e.g., Azurite emulator), set the endpoint explicitly.
        if (!endpoint.storage_account_url.empty() && endpoint.storage_account_url.starts_with("http://"))
            set_option("azure_endpoint", endpoint.storage_account_url);

        LOG_TRACE(
            log,
            "Using storage_account_url: {}, container: {}, data_path: {}",
            endpoint.storage_account_url, endpoint.container_name, data_path);

        return builder;
    }

private:
    const DB::AzureBlobStorage::ConnectionParams connection_params;
    const std::string table_location;
    const std::string data_path;
    const LoggerPtr log = getLogger("AzureKernelHelper");

    static std::string buildTableLocation(
        const DB::AzureBlobStorage::ConnectionParams & params,
        const std::string & blob_path)
    {
        auto path = blob_path;
        if (!path.empty() && path.front() == '/')
            path = path.substr(1);
        return "az://" + params.endpoint.container_name + "/" + path;
    }
};
#endif

/// A helper class to manage local fs storage.
class LocalKernelHelper final : public IKernelHelper
{
public:
    explicit LocalKernelHelper(const std::string & path_) : table_location(getTableLocation(path_)), path(path_) {}

    const std::string & getTableLocation() const override { return table_location; }

    const std::string & getDataPath() const override { return path; }

    ffi::EngineBuilder * createBuilder() const override
    {
        ffi::EngineBuilder * builder = KernelUtils::unwrapResult(
            ffi::get_engine_builder(
                KernelUtils::toDeltaString(table_location),
                &KernelUtils::allocateError),
            "get_engine_builder");

        return builder;
    }

private:
    const std::string table_location;
    const std::string path;

    static std::string getTableLocation(const std::string & path)
    {
        return "file://" + path + "/";
    }
};
}

namespace DB
{

namespace S3AuthSetting
{
    extern const S3AuthSettingsString access_key_id;
    extern const S3AuthSettingsString secret_access_key;
    extern const S3AuthSettingsString region;
}

DeltaLake::KernelHelperPtr getKernelHelper(
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage)
{
    switch (configuration->getType())
    {
        case DB::ObjectStorageType::S3:
        {
            const auto * s3_conf = dynamic_cast<const DB::StorageS3Configuration *>(configuration.get());
            return std::make_shared<DeltaLake::S3KernelHelper>(
                s3_conf->url,
                object_storage->getS3StorageClient(),
                s3_conf->getAuthSettings());
        }
#if USE_AZURE_BLOB_STORAGE
        case DB::ObjectStorageType::Azure:
        {
            return std::make_shared<DeltaLake::AzureKernelHelper>(
                object_storage->getAzureBlobStorageConnectionParams(),
                configuration->getRawPath().path);
        }
#endif
        case DB::ObjectStorageType::Local:
        {
            const auto * local_conf = dynamic_cast<const DB::StorageLocalConfiguration *>(configuration.get());
            return std::make_shared<DeltaLake::LocalKernelHelper>(local_conf->getPathForRead().path);
        }
        default:
        {
            throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                                "Unsupported storage type: {}", configuration->getType());
        }
    }
}

}

#endif
