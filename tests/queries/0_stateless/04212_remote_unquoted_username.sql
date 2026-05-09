-- Tags: shard

-- An unquoted identifier is accepted as a user name in `remote` and `remoteSecure`,
-- which is consistent with how the database and table arguments work. Previously,
-- a bare identifier in this position was silently treated as a sharding key, which
-- led to a confusing authentication error referring to the `default` user.
-- See https://github.com/ClickHouse/ClickHouse/issues/33816

-- Identifier as user name with `db.table` form.
SELECT * FROM remote('127.0.0.1', system.one, default) FORMAT Null;

-- Identifier as user name with separate database and table.
SELECT * FROM remote('127.0.0.1', system, one, default) FORMAT Null;

-- Identifier as user name, then string-literal password.
SELECT * FROM remote('127.0.0.1', system.one, default, '') FORMAT Null;
SELECT * FROM remote('127.0.0.1', system, one, default, '') FORMAT Null;
SELECT * FROM remote('127.0.0.1', system.one, default, '', identity(dummy)) FORMAT Null;

-- The password must still be a string literal: a non-literal in the password
-- position falls through to the sharding key, which is verified by 02841.
