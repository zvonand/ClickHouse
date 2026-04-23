#pragma once

#include <string>

namespace DB::S3
{
enum class AZFacilities
{
    AUTO,
    AWS_ZONE_ID,
    AWS_ZONE_NAME,
    GCP_ZONE,
    CLICKHOUSE
};

/// getRunningAvailabilityZone returns the availability zone of the underlying compute resources where the current process runs.
std::string getRunningAvailabilityZone(AZFacilities az_facility = AZFacilities::AUTO);
std::string tryGetRunningAvailabilityZone(AZFacilities az_facility = AZFacilities::AUTO);
}
