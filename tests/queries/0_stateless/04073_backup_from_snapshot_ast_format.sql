-- Test that BACKUP FROM SNAPSHOT AST round-trips correctly through format and parse.
-- Previously, formatQueryImpl did not emit the FROM SNAPSHOT clause, causing
-- "Inconsistent AST formatting" server exceptions.

SELECT formatQuery('BACKUP FROM SNAPSHOT S3(\'http://localhost:9000/bucket/snapshot/\') TO S3(\'http://localhost:9000/bucket/backup/\')');
SELECT formatQuery('BACKUP FROM SNAPSHOT S3(\'http://localhost:9000/bucket/snapshot/\') TO S3(\'http://localhost:9000/bucket/backup/\') SETTINGS id = \'abc\'');
SELECT formatQuery('BACKUP FROM SNAPSHOT Disk(\'default\', \'/snapshot/\') TO Disk(\'default\', \'/backup/\')');
