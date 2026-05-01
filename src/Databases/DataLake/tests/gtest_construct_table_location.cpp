#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <base/types.h>

namespace DataLake::Test
{

class ConstructTableLocationTest : public ::testing::Test
{
};

/// S3: bucket is the first path segment of the HTTPS-style storage_endpoint.
TEST_F(ConstructTableLocationTest, S3HttpsEndpoint)
{
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse-rest", "ns", "tbl"),
        "s3://warehouse-rest/ns/tbl");
}

/// S3: storage_endpoint may carry a sub-prefix that must be preserved.
TEST_F(ConstructTableLocationTest, S3HttpsEndpointWithSubPrefix)
{
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse/data", "ns", "tbl"),
        "s3://warehouse/data/ns/tbl");
}

TEST_F(ConstructTableLocationTest, S3RejectsEndpointWithoutBucket)
{
    EXPECT_THROW(
        constructTableLocation("s3", "http://minio:9000/", "ns", "tbl"),
        DB::Exception);
}

/// Azure: HTTPS-form storage_endpoint must round-trip through `setLocation`,
/// which means the constructed URI must include the `<container>@<host>` authority.
TEST_F(ConstructTableLocationTest, AzureHttpsEndpoint)
{
    const String location = constructTableLocation(
        "abfss",
        "https://account.dfs.core.windows.net/mycontainer",
        "ns",
        "tbl");
    EXPECT_EQ(location, "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");

    /// Verify the produced URI parses back into the expected components.
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation(location);
    EXPECT_EQ(metadata.getLocation(), location);
    EXPECT_EQ(metadata.getStorageType(), StorageType::Azure);
}

TEST_F(ConstructTableLocationTest, AzureHttpsEndpointWithSubPath)
{
    const String location = constructTableLocation(
        "abfss",
        "https://account.dfs.core.windows.net/mycontainer/warehouse/data",
        "ns",
        "tbl");
    EXPECT_EQ(
        location,
        "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data/ns/tbl");

    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation(location);
    EXPECT_EQ(metadata.getLocation(), location);
}

TEST_F(ConstructTableLocationTest, AzureHttpsEndpointTrailingSlash)
{
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "https://account.dfs.core.windows.net/mycontainer/",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");
}

/// Azure: ABFSS-form storage_endpoint is also accepted.
TEST_F(ConstructTableLocationTest, AzureAbfssEndpoint)
{
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "abfss://mycontainer@account.dfs.core.windows.net/",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");
}

TEST_F(ConstructTableLocationTest, AzureAbfssEndpointWithSubPath)
{
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data/ns/tbl");
}

TEST_F(ConstructTableLocationTest, AzureRejectsEndpointWithoutContainer)
{
    EXPECT_THROW(
        constructTableLocation("abfss", "https://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
    EXPECT_THROW(
        constructTableLocation("abfss", "abfss://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
}

/// HDFS: the authority (host:port) must be preserved in the location URI.
TEST_F(ConstructTableLocationTest, HdfsPreservesAuthority)
{
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000/warehouse", "ns", "tbl"),
        "hdfs://namenode:9000/warehouse/ns/tbl");
}

/// `file://` URIs have an empty authority and just a local filesystem path.
TEST_F(ConstructTableLocationTest, FileWithoutAuthority)
{
    EXPECT_EQ(
        constructTableLocation("file", "file:///var/iceberg/warehouse", "ns", "tbl"),
        "file:///var/iceberg/warehouse/ns/tbl");
}

}
