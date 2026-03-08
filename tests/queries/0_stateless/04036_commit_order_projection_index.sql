-- Tags: no-random-merge-tree-settings

drop table if exists mt_commit_order_idx sync;

CREATE TABLE mt_commit_order_idx(
    a UInt64,
    b UInt64,
    projection commit_order index b type commit_order
)
ENGINE = MergeTree
ORDER BY a
settings enable_block_number_column=1, enable_block_offset_column=1, index_granularity=1;

insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
insert into mt_commit_order_idx select rand(), rand() from numbers(10);
optimize table mt_commit_order_idx final;

select 'reading all columns';
explain indexes=1, projections=1 select *, _block_number, _block_offset from mt_commit_order_idx where (_block_number, _block_offset) = (3, 6);

select '';
select 'reading indexed columns';
explain indexes=1, projections=1 select b, _block_number, _block_offset from mt_commit_order_idx where (_block_number, _block_offset) = (3, 6);
