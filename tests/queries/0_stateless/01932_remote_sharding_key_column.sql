-- Tags: shard

-- Regression test for the following query:
--
--     select * from remote('127.1', system.one, identity(dummy))
--
-- that produced the following error before:
--
--     Unknown column: dummy, there are only columns .
--
-- A bare identifier at the same position (e.g. `dummy`) is interpreted as
-- the user name, so we wrap the column reference in a function to make the
-- intent unambiguous.
select * from remote('127.1', system.one, identity(dummy)) format Null;
select * from remote('127.1', view(select * from system.one), identity(dummy)) format Null;
select * from remote('127.{1,2}', view(select * from system.one), identity(dummy)) format Null;
