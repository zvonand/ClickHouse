-- Tests for ALTER TABLE ... EXECUTE coverage gaps from PR #97904:
-- (1) Multiple EXECUTE commands in one ALTER statement
-- (2) Expression arguments in EXECUTE commands
-- (3) AST clone/format roundtrip via EXPLAIN AST (exercises ASTAlterCommand::clone for execute_args)

-- Multiple EXECUTE commands in one ALTER statement
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd1(), EXECUTE cmd2()');

-- EXECUTE with expression argument
SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE cmd(1 + 2)');

-- EXPLAIN AST exercises ASTAlterCommand::clone() for execute_args
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots('2024-06-01 00:00:00');
EXPLAIN AST ALTER TABLE t EXECUTE expire_snapshots();
EXPLAIN AST ALTER TABLE t EXECUTE some_cmd('a', 42, 3.14);
