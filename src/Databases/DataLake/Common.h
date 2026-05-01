#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

namespace DataLake
{

String trim(const String & str);

std::vector<String> splitTypeArguments(const String & type_str);

DB::DataTypePtr getType(const String & type_name, bool nullable, const String & prefix = "");

/// Parse a string, containing at least one dot, into a two substrings:
/// A.B.C.D.E -> A.B.C.D and E, where
/// `A.B.C.D` is a table "namespace".
/// `E` is a table name.
std::pair<std::string, std::string> parseTableName(const std::string & name);

/// Build the canonical Iceberg table-`location` URI for a newly created table.
///
/// The constructed URI is what gets persisted into the table's metadata file (the `location`
/// field) and what every later read round-trips through `TableMetadata::setLocation`. So it must
/// be in the exact form expected by `setLocation` and by the per-backend parsers in
/// `src/Storages/ObjectStorage/.../Configuration.cpp`. In particular, for non-S3 schemes the URI
/// authority of `storage_endpoint` must be preserved — otherwise (for example) an Azure endpoint
/// like `https://account.dfs.core.windows.net/container` would yield `abfss://container/ns/table`,
/// which is missing the required `@account.dfs.core.windows.net` authority.
String constructTableLocation(
    const String & location_scheme,
    const String & storage_endpoint,
    const String & namespace_name,
    const String & table_name);

}
