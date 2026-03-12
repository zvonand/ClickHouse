-- Tests for ALTER TABLE ... EXECUTE coverage gaps from PR #97904:
-- (1) ALTER EXECUTE privilege hierarchy
-- (2) Multiple EXECUTE commands in one ALTER statement
-- (3) Expression arguments in EXECUTE commands
-- (4) AST clone/format roundtrip via EXPLAIN AST

-- Verify ALTER EXECUTE privilege is listed under ALTER TABLE
SELECT privilege, parent_group FROM system.privileges WHERE privilege = 'ALTER EXECUTE' ORDER BY privilege;

-- Multiple EXECUTE commands in one ALTER statement
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd1(), EXECUTE cmd2()');

-- EXECUTE with expression argument
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd(1 + 2)');

-- EXPLAIN AST exercises ASTAlterCommand::clone() for execute_args
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots('2024-06-01 00:00:00');
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots();
EXPLAIN AST ALTER TABLE t EXECUTE some_cmd('a', 42, 3.14);

-- Multiple EXECUTE on MergeTree should fail with NOT_IMPLEMENTED
DROP TABLE IF EXISTS test_exec_multi_04038;
CREATE TABLE test_exec_multi_04038 (x UInt32) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test_exec_multi_04038 EXECUTE cmd1(), EXECUTE cmd2(); -- { serverError NOT_IMPLEMENTED }
DROP TABLE IF EXISTS test_exec_multi_04038;
