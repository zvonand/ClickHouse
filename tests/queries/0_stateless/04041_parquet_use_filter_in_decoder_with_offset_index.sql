-- Tags: no-fasttest
-- Regression test for nullptr dereference in Parquet reader when use_filter_in_decoder
-- path encounters pages with reset prefetch handles (filtered out by offset index).
-- https://github.com/ClickHouse/ClickHouse/issues/99676

set output_format_parquet_use_custom_encoder = 1;
set input_format_parquet_use_native_reader_v3 = 1;
set engine_file_truncate_on_insert = 1;

-- Create a parquet file with offset index and multiple small pages per row group.
-- Use small data_page_size and batch_size to ensure multiple pages are created.
insert into function file(currentDatabase() || '04041.parquet')
    select number as n, toString(number) as s
    from numbers(1000)
    settings output_format_parquet_data_page_size = 100,
             output_format_parquet_batch_size = 10,
             output_format_parquet_row_group_size = 1000,
             output_format_parquet_write_page_index = 1;

-- Query with a filter that skips most pages entirely, so that
-- determinePagesToPrefetch resets their prefetch handles.
-- The WHERE clause must pass through a non-dictionary-encoded column to trigger
-- the use_filter_in_decoder code path.
select s
    from file(currentDatabase() || '04041.parquet')
    where n in (5, 500, 995)
    order by n
    settings input_format_parquet_max_block_size = 10;
