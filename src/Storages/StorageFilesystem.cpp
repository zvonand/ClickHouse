#include <chrono>
#include <filesystem>
#include <mutex>
#include <unordered_set>
#include <base/types.h>
#include <Core/DecimalFunctions.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageFilesystem.h>
#include <Storages/StorageFactory.h>
#include <Storages/VirtualColumnUtils.h>
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
    return 9; /// unknown
}

struct QueueEntry
{
    fs::directory_entry entry;
    UInt16 depth;
};

class FilesystemSource final : public ISource
{
public:
    struct PathInfo
    {
        ConcurrentBoundedQueue<QueueEntry> queue;
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
        auto names_and_types_in_use = storage_snapshot->getSampleBlockForColumns(columns_in_use).getNamesAndTypesList();

        /// Phase 1: Collect entries from the queue and expand directories.
        std::vector<QueueEntry> entries;
        entries.reserve(max_block_size);

        while (entries.size() < max_block_size)
        {
            if (isCancelled())
                break;

            QueueEntry queue_entry;
            if (!path_info->queue.pop(queue_entry))
                break;

            auto & file = queue_entry.entry;
            UInt16 depth = queue_entry.depth;

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
                    if (!path_info->queue.push(QueueEntry{child, static_cast<UInt16>(depth + 1)}))
                    {
                        path_info->in_flight.fetch_sub(1);
                        LOG_WARNING(&Poco::Logger::get("StorageFilesystem"), "Too many files to process, skipping some from {}",
                            file.path().string());
                    }
                }

                /// Directories themselves are also returned as rows.
            }

            entries.push_back(std::move(queue_entry));
        }

        if (entries.empty())
            return {};

        /// Phase 2: If we have a filter on cheap columns, evaluate it to get a bitmask.
        /// Then only process expensive columns (content, size, modification_time, permissions) for surviving entries.

        std::vector<bool> mask(entries.size(), true);

        if (filter_expression)
        {
            /// Fill cheap columns for the filter.
            Block cheap_block;
            for (size_t col_idx = 0; col_idx < filter_sample_block.columns(); ++col_idx)
            {
                const auto & col_info = filter_sample_block.getByPosition(col_idx);
                auto column = col_info.type->createColumn();

                for (const auto & queue_entry : entries)
                {
                    const auto & file = queue_entry.entry;
                    UInt16 depth = queue_entry.depth;
                    std::error_code ec;

                    if (col_info.name == "path")
                        column->insert(file.path().parent_path().string());
                    else if (col_info.name == "name")
                        column->insert(file.path().filename().string());
                    else if (col_info.name == "depth")
                        column->insert(static_cast<UInt16>(depth > 0 ? depth - 1 : 0));
                    else if (col_info.name == "type")
                    {
                        auto status = file.status(ec);
                        if (ec.value() == 0)
                            column->insert(fileTypeToEnumValue(status.type()));
                        else
                        {
                            column->insert(Int8(9));
                            ec.clear();
                        }
                    }
                    else if (col_info.name == "is_symlink")
                    {
                        column->insert(file.is_symlink(ec));
                        ec.clear();
                    }
                }

                cheap_block.insert(ColumnWithTypeAndName(std::move(column), col_info.type, col_info.name));
            }

            /// Execute the filter expression to produce a boolean result column.
            filter_expression->execute(cheap_block);

            /// The result column is the last one added by the expression.
            const auto & result_name = filter_expression->getSampleBlock().getByPosition(
                filter_expression->getSampleBlock().columns() - 1).name;
            const auto & result_column = cheap_block.getByName(result_name).column;

            for (size_t i = 0; i < entries.size(); ++i)
                mask[i] = result_column->getBool(i);
        }

        /// Phase 3: Fill output columns, skipping expensive work for filtered-out entries.

        std::unordered_map<String, MutableColumnPtr> columns_map;
        for (const auto & [name, type] : names_and_types_in_use)
            columns_map[name] = type->createColumn();

        bool need_content = columns_map.contains("content");

        for (size_t i = 0; i < entries.size(); ++i)
        {
            if (!mask[i])
            {
                /// Decrement in_flight for filtered-out entries.
                if (path_info->in_flight.fetch_sub(1) == 1)
                    path_info->queue.finish();
                continue;
            }

            const auto & file = entries[i].entry;
            UInt16 depth = entries[i].depth;
            std::error_code ec;

            if (columns_map.contains("type"))
            {
                auto status = file.status(ec);
                if (ec.value() == 0)
                    columns_map["type"]->insert(fileTypeToEnumValue(status.type()));
                else
                {
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
                columns_map["path"]->insert(file.path().parent_path().string());

            if (columns_map.contains("name"))
                columns_map["name"]->insert(file.path().filename().string());

            if (columns_map.contains("depth"))
                columns_map["depth"]->insert(static_cast<UInt16>(depth > 0 ? depth - 1 : 0));

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
                    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                        sys_clock_file_time.time_since_epoch()).count();
                    columns_map["modification_time"]->insert(DecimalField<DateTime64>(microseconds, 6));
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
            columns.emplace_back(std::move(columns_map[name]));

        return {std::move(columns), num_rows};
    }

    FilesystemSource(
        const StorageSnapshotPtr & metadata_snapshot_, UInt64 max_block_size_, PathInfoPtr path_info_, Names column_names,
        ExpressionActionsPtr filter_expression_, Block filter_sample_block_)
        : ISource(std::make_shared<const Block>(metadata_snapshot_->getSampleBlockForColumns(column_names)))
        , storage_snapshot(metadata_snapshot_)
        , path_info(std::move(path_info_))
        , max_block_size(max_block_size_)
        , columns_in_use(std::move(column_names))
        , filter_expression(std::move(filter_expression_))
        , filter_sample_block(std::move(filter_sample_block_))
    {
    }

private:
    StorageSnapshotPtr storage_snapshot;
    PathInfoPtr path_info;
    UInt64 max_block_size;
    Names columns_in_use;
    ExpressionActionsPtr filter_expression;
    Block filter_sample_block;

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


class ReadFromFilesystem final : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromFilesystem"; }

    ReadFromFilesystem(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        bool local_mode_,
        String path_,
        String user_files_absolute_path_string_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(
            std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(column_names_)),
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , local_mode(local_mode_)
        , path(std::move(path_))
        , user_files_absolute_path_string(std::move(user_files_absolute_path_string_))
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

    void applyFilters(ActionDAGNodes added_filter_nodes) override
    {
        SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

        if (!filter_actions_dag)
            return;

        /// Build a sample block with "cheap" columns that can be used for early filtering.
        /// These columns can be obtained without stat/read syscalls (just from the directory entry name and traversal depth).
        Block cheap_sample;
        cheap_sample.insert({DataTypeFactory::instance().get("String")->createColumn(),
                             DataTypeFactory::instance().get("String"), "path"});
        cheap_sample.insert({DataTypeFactory::instance().get("String")->createColumn(),
                             DataTypeFactory::instance().get("String"), "name"});
        cheap_sample.insert({DataTypeFactory::instance().get("UInt16")->createColumn(),
                             DataTypeFactory::instance().get("UInt16"), "depth"});
        cheap_sample.insert({storage_snapshot->metadata->getSampleBlock().getByName("type").type->createColumn(),
                             storage_snapshot->metadata->getSampleBlock().getByName("type").type, "type"});
        cheap_sample.insert({DataTypeFactory::instance().get("Bool")->createColumn(),
                             DataTypeFactory::instance().get("Bool"), "is_symlink"});

        auto filter_dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(
            filter_actions_dag->getOutputs().at(0), &cheap_sample, context);

        if (filter_dag)
        {
            VirtualColumnUtils::buildSetsForDAG(*filter_dag, context);
            filter_expression = VirtualColumnUtils::buildFilterExpression(std::move(*filter_dag), context);
            filter_sample_block = std::move(cheap_sample);
        }
    }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
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
        if (!path_info->queue.push(QueueEntry{fs::directory_entry(file_path), 0}))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot schedule a file '{}'", file_path.string());

        Pipes pipes;
        for (size_t i = 0; i < num_streams; ++i)
        {
            pipes.emplace_back(std::make_shared<FilesystemSource>(
                storage_snapshot, max_block_size, path_info, required_source_columns,
                filter_expression, filter_sample_block));
        }
        auto pipe = Pipe::unitePipes(std::move(pipes));
        pipeline.init(std::move(pipe));
    }

private:
    bool local_mode;
    String path;
    String user_files_absolute_path_string;
    size_t max_block_size;
    size_t num_streams;
    ExpressionActionsPtr filter_expression;
    Block filter_sample_block;
};

}


void StorageFilesystem::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t max_block_size,
    size_t num_streams)
{
    auto reading = std::make_unique<ReadFromFilesystem>(
        column_names, query_info, storage_snapshot, context,
        local_mode, path, user_files_absolute_path_string,
        max_block_size, num_streams);

    query_plan.addStep(std::move(reading));
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
