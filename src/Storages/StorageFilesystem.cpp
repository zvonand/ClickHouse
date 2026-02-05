#include <chrono>
#include <filesystem>
#include <mutex>
#include <unordered_set>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageFilesystem.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

/// Must match the Enum8 values in TableFunctionFilesystem::getActualTableStructure.
Int8 fileTypeToEnumValue(fs::file_type type)
{
    switch (type)
    {
        case fs::file_type::none:       return 0;
        case fs::file_type::not_found:  return 1;
        case fs::file_type::regular:    return 2;
        case fs::file_type::directory:  return 3;
        case fs::file_type::symlink:    return 4;
        case fs::file_type::block:      return 5;
        case fs::file_type::character:  return 6;
        case fs::file_type::fifo:       return 7;
        case fs::file_type::socket:     return 8;
        case fs::file_type::unknown:    return 9;
    }
}

class FilesystemSource final : public ISource
{
public:
    struct PathInfo
    {
        ConcurrentBoundedQueue<fs::directory_entry> queue;
        std::atomic<int64_t> in_flight{0};
        std::mutex visited_mutex;
        std::unordered_set<String> visited;
        const String user_files_absolute_path_string;
        const bool need_check;

        PathInfo(String user_files_absolute_path_string_, bool need_check_)
            : queue(10000)
            , user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
            , need_check(need_check_)
        {
        }
    };
    using PathInfoPtr = std::shared_ptr<PathInfo>;

    String getName() const override { return "Filesystem"; }

    Chunk generate() override
    {
        size_t current_block_size = 0;
        std::unordered_map<String, MutableColumnPtr> columns_map;
        auto names_and_types_in_use = storage_snapshot->getSampleBlockForColumns(columns_in_use).getNamesAndTypesList();
        for (const auto & [name, type] : names_and_types_in_use)
        {
            columns_map[name] = type->createColumn();
        }

        bool need_content = columns_map.contains("content");

        fs::directory_entry file;
        while (current_block_size < max_block_size)
        {
            if (isCancelled())
                break;

            if (!path_info->queue.pop(file))
                break;

            std::error_code ec;

            if (file.is_directory(ec) && ec.value() == 0)
            {
                for (const auto & child : fs::directory_iterator(file, ec))
                {
                    fs::path child_path = fs::absolute(child.path()).lexically_normal();

                    if (path_info->need_check && !fileOrSymlinkPathStartsWith(child_path.string(), path_info->user_files_absolute_path_string))
                    {
                        LOG_DEBUG(&Poco::Logger::get("StorageFilesystem"), "Path {} is not inside user_files {}",
                            child_path.string(), path_info->user_files_absolute_path_string);
                        continue;
                    }

                    /// Symlink cycle detection: for directories that are symlinks, check canonical path.
                    if (child.is_directory(ec) && child.is_symlink(ec))
                    {
                        std::error_code canon_ec;
                        auto canonical = fs::canonical(child_path, canon_ec);
                        if (!canon_ec)
                        {
                            std::lock_guard lock(path_info->visited_mutex);
                            if (!path_info->visited.emplace(canonical.string()).second)
                                continue;
                        }
                    }

                    path_info->in_flight.fetch_add(1);
                    if (!path_info->queue.push(child))
                    {
                        path_info->in_flight.fetch_sub(1);
                        LOG_WARNING(&Poco::Logger::get("StorageFilesystem"), "Too many files to process, skipping some from {}",
                            file.path().string());
                    }
                }

                /// Directories themselves are also returned as rows.
            }

            current_block_size++;

            if (columns_map.contains("type"))
            {
                auto status = file.status(ec);
                if (ec.value() == 0)
                    columns_map["type"]->insert(fileTypeToEnumValue(status.type()));
                else
                {
                    /// 'unknown' on error
                    columns_map["type"]->insert(Int8(9));
                    ec.clear();
                }
            }

            if (columns_map.contains("is_symlink"))
            {
                columns_map["is_symlink"]->insert(file.is_symlink(ec));
                ec.clear();
            }

            if (columns_map.contains("path"))
            {
                columns_map["path"]->insert(file.path().string());
            }

            if (columns_map.contains("name"))
            {
                columns_map["name"]->insert(file.path().filename().string());
            }

            if (columns_map.contains("size"))
            {
                auto is_regular_file = file.is_regular_file(ec);
                if (ec.value() == 0 && is_regular_file)
                {
                    auto file_size = file.file_size(ec);
                    if (ec.value() == 0)
                        columns_map["size"]->insert(file_size);
                    else
                    {
                        columns_map["size"]->insertDefault();
                        ec.clear();
                    }
                }
                else
                {
                    columns_map["size"]->insertDefault();
                    ec.clear();
                }
            }

            if (columns_map.contains("modification_time"))
            {
                auto file_time = fs::last_write_time(file.path(), ec);
                if (ec.value() == 0)
                {
                    auto sys_clock_file_time = std::chrono::file_clock::to_sys(file_time);
                    auto sys_clock_in_seconds_duration = std::chrono::time_point_cast<std::chrono::seconds>(sys_clock_file_time);
                    auto file_time_since_epoch = sys_clock_in_seconds_duration.time_since_epoch().count();
                    columns_map["modification_time"]->insert(file_time_since_epoch);
                }
                else
                {
                    columns_map["modification_time"]->insertDefault();
                    ec.clear();
                }
            }

            if (need_content)
            {
                auto is_regular = file.is_regular_file(ec);
                if (ec.value() == 0 && is_regular)
                {
                    try
                    {
                        String content;
                        ReadBufferFromFile in(file.path().string());
                        readStringUntilEOF(content, in);
                        columns_map["content"]->insert(std::move(content));
                    }
                    catch (...)
                    {
                        columns_map["content"]->insertDefault();
                    }
                }
                else
                {
                    columns_map["content"]->insertDefault();
                    ec.clear();
                }
            }

            for (const auto & [column_name, perm] : permissions_columns_names)
            {
                if (!columns_map.contains(column_name))
                    continue;
                auto status = file.status(ec);
                if (ec.value() == 0)
                    columns_map[column_name]->insert(static_cast<bool>(status.permissions() & perm));
                else
                {
                    columns_map[column_name]->insertDefault();
                    ec.clear();
                }
            }

            /// Decrement in_flight after processing the item.
            if (path_info->in_flight.fetch_sub(1) == 1)
            {
                /// We were the last item; signal all streams to stop.
                path_info->queue.finish();
            }
        }

        auto num_rows = columns_map.begin() != columns_map.end() ? columns_map.begin()->second->size() : 0;

        if (num_rows == 0)
            return {};

        Columns columns;
        for (const auto & [name, _] : names_and_types_in_use)
        {
            columns.emplace_back(std::move(columns_map[name]));
        }

        return {std::move(columns), num_rows};
    }

    FilesystemSource(
        const StorageSnapshotPtr & metadata_snapshot_, UInt64 max_block_size_, PathInfoPtr path_info_, Names column_names)
        : ISource(std::make_shared<const Block>(metadata_snapshot_->getSampleBlockForColumns(column_names)))
        , storage_snapshot(metadata_snapshot_)
        , path_info(std::move(path_info_))
        , max_block_size(max_block_size_)
        , columns_in_use(std::move(column_names))
    {
    }

private:
    StorageSnapshotPtr storage_snapshot;
    PathInfoPtr path_info;
    UInt64 max_block_size;
    Names columns_in_use;

    const std::vector<std::pair<String, fs::perms>> permissions_columns_names
    {
        {"owner_read", fs::perms::owner_read},
        {"owner_write", fs::perms::owner_write},
        {"owner_exec", fs::perms::owner_exec},
        {"group_read", fs::perms::group_read},
        {"group_write", fs::perms::group_write},
        {"group_exec", fs::perms::group_exec},
        {"others_read", fs::perms::others_read},
        {"others_write", fs::perms::others_write},
        {"others_exec", fs::perms::others_exec},
        {"set_gid", fs::perms::set_gid},
        {"set_uid", fs::perms::set_uid},
        {"sticky_bit", fs::perms::sticky_bit}
    };
};

}


Pipe StorageFilesystem::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t num_streams)
{
    auto path_info = std::make_shared<FilesystemSource::PathInfo>(user_files_absolute_path_string, !local_mode);

    fs::path file_path(path);
    if (file_path.is_relative())
        file_path = fs::path(path_info->user_files_absolute_path_string) / file_path;
    file_path = fs::absolute(file_path).lexically_normal();

    if (path_info->need_check && !fileOrSymlinkPathStartsWith(file_path.string(), path_info->user_files_absolute_path_string))
        throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED, "Path {} is not inside user_files {}",
            file_path.string(), path_info->user_files_absolute_path_string);

    if (!fs::exists(file_path))
    {
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory {} doesn't exist", file_path.string());
    }

    /// Register the root directory as visited (by canonical path) to prevent symlink cycles.
    {
        std::error_code canon_ec;
        auto canonical = fs::canonical(file_path, canon_ec);
        if (!canon_ec)
            path_info->visited.emplace(canonical.string());
    }

    /// in_flight starts at 1 for the root entry.
    path_info->in_flight.store(1);
    if (!path_info->queue.push(fs::directory_entry(file_path)))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot schedule a file '{}'", file_path.string());

    Pipes pipes;
    for (size_t i = 0; i < num_streams; ++i)
    {
        pipes.emplace_back(std::make_shared<FilesystemSource>(storage_snapshot, max_block_size, path_info, column_names));
    }
    auto pipe = Pipe::unitePipes(std::move(pipes));
    return pipe;
}

StorageFilesystem::StorageFilesystem(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool local_mode_,
    String path_,
    String user_files_absolute_path_string_
    )
    : IStorage(table_id_), local_mode(local_mode_), path(std::move(path_)), user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_);
    metadata.setConstraints(constraints_);
    metadata.setComment(comment);
    setInMemoryMetadata(metadata);
}

Strings StorageFilesystem::getDataPaths() const
{
    return {path};
}


void registerStorageFilesystem(StorageFactory & factory)
{
    factory.registerStorage("Filesystem", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage Filesystem requires one argument: path.");

        String path;

        if (!engine_args.empty())
        {
            const auto & ast_literal = engine_args.front()->as<const ASTLiteral &>();
            if (!ast_literal.value.isNull())
                path = checkAndGetLiteralArgument<String>(ast_literal, "path");
        }

        String user_files_absolute_path_string = fs::canonical(fs::path(args.getContext()->getUserFilesPath()).string());
        bool local_mode = args.getContext()->getApplicationType() == Context::ApplicationType::LOCAL;

        return std::make_shared<StorageFilesystem>(
            args.table_id, args.columns, args.constraints, args.comment, local_mode, path,
            user_files_absolute_path_string);
    },
    {
        .source_access_type = AccessTypeObjects::Source::FILE,
    });
}
}
