#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROFILE1="s1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PROFILE2="s2_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${PROFILE1}, ${PROFILE2}"

# Run the main block with --ignore-error to keep session state across expected errors.
# Errors go to stderr which is discarded, matching the original .sql test behavior.
${CLICKHOUSE_CLIENT} --ignore-error -n -q "
SELECT '--- assigning ---';
SET custom_a = 5;
SET custom_b = -177;
SET custom_c = 98.11;
SET custom_d = 'abc def';
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '--- modifying ---';
SET custom_a = 'changed';
SET custom_b = NULL;
SET custom_c = 50000;
SET custom_d = 1.11;
SELECT getSetting('custom_a') as v, toTypeName(v);
SELECT getSetting('custom_b') as v, toTypeName(v);
SELECT getSetting('custom_c') as v, toTypeName(v);
SELECT getSetting('custom_d') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name LIKE 'custom_%' ORDER BY name;

SELECT '--- undefined setting ---';
SELECT getSetting('custom_e') as v, toTypeName(v);
SET custom_e = 404;
SELECT getSetting('custom_e') as v, toTypeName(v);

SELECT '--- wrong prefix ---';
SET invalid_custom = 8;

SELECT '--- using query context ---';
SELECT getSetting('custom_e') as v, toTypeName(v) SETTINGS custom_e = -0.333;
SELECT name, value FROM system.settings WHERE name = 'custom_e' SETTINGS custom_e = -0.333;
SELECT getSetting('custom_e') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_e';

SELECT getSetting('custom_f') as v, toTypeName(v) SETTINGS custom_f = 'word';
SELECT name, value FROM system.settings WHERE name = 'custom_f' SETTINGS custom_f = 'word';
SELECT getSetting('custom_f') as v, toTypeName(v);
SELECT COUNT() FROM system.settings WHERE name = 'custom_f';

SELECT '--- compound identifier ---';
SET custom_compound.identifier.v1 = 'test';
SELECT getSetting('custom_compound.identifier.v1') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_compound.identifier.v1';
" 2>/dev/null

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE1} SETTINGS custom_compound.identifier.v2 = 100"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${PROFILE1}" | sed "s/${PROFILE1}/PROFILE1/g"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE ${PROFILE1}"

${CLICKHOUSE_CLIENT} -n -q "
SELECT '--- null type ---';
SELECT getSetting('custom_null') as v, toTypeName(v) SETTINGS custom_null = NULL;
SELECT name, value FROM system.settings WHERE name = 'custom_null' SETTINGS custom_null = NULL;

SET custom_null = NULL;
SELECT getSetting('custom_null') as v, toTypeName(v);
SELECT name, value FROM system.settings WHERE name = 'custom_null';
"

${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${PROFILE2} SETTINGS custom_null = NULL"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE SETTINGS PROFILE ${PROFILE2}" | sed "s/${PROFILE2}/PROFILE2/g"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE ${PROFILE2}"
