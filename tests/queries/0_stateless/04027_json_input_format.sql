-- Test auto case
DROP TABLE IF EXISTS json_test;

CREATE TABLE json_test (id Int, name String);

SET input_format_with_names_case_insensitive_column_matching='auto';

INSERT INTO json_test FORMAT JSONEachRow {"id": 0, "name": "aa"} {"ID": 1, "NAME": "bb"}

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test auto case ambiguity
CREATE TABLE json_test (age Int, AGE Int);

SET input_format_with_names_case_insensitive_column_matching='auto';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10};

INSERT INTO json_test FORMAT JSONEachRow {"AgE": 1, "aGe": 20}; -- { clientError INCORRECT_DATA }

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test match case
CREATE TABLE json_test (age Int, AGE Int);

SET input_format_with_names_case_insensitive_column_matching='match_case';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10} {"AGE": 20, "age": 1};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ignore case
CREATE TABLE json_test (id Int, age Int);

SET input_format_with_names_case_insensitive_column_matching='ignore_case';

INSERT INTO json_test FORMAT JSONEachRow {"ID": 0, "AGE": 10};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ignore case ambiguity
CREATE TABLE json_test (AGE Int, age Int, id Int);

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10}; -- { clientError 117}

INSERT INTO json_test FORMAT JSONEachRow {"id": 0};

SELECT * FROM json_test;

DROP TABLE json_test;