-- Tags: no-fasttest
-- no-fasttest: requires ICU library

-- Negative tests: parameter validation
SELECT caseFoldUTF8(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT caseFoldUTF8('x', 'aggressive', 1, 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT accentFoldUTF8('x', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT foldUTF8('x', 'aggressive', 1); -- { serverError BAD_ARGUMENTS}
SELECT caseFoldUTF8(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 'aggressive', 'true'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT caseFoldUTF8('x', 'invalid'); -- { serverError BAD_ARGUMENTS }
SELECT caseFoldUTF8('x', 'aggressive', 1); -- { serverError BAD_ARGUMENTS }
SELECT foldUTF8('x', 'invalid'); -- { serverError BAD_ARGUMENTS }
SELECT caseFoldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT accentFoldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT foldUTF8(toFixedString('hello', 5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- caseFoldUTF8: basic case folding
SELECT '-- caseFoldUTF8 aggressive (default)';
SELECT caseFoldUTF8('Hello World');
SELECT caseFoldUTF8('Straße');
SELECT caseFoldUTF8('HÉLLO');
SELECT caseFoldUTF8('ﬃ'); -- ffi ligature: aggressive NFKC decomposes it
SELECT caseFoldUTF8('Ⅷ') AS aggressive, caseFoldUTF8('Ⅷ', 'conservative') AS conservative; -- Roman numeral: aggressive decomposes, conservative lowercases

SELECT '-- caseFoldUTF8 conservative';
SELECT caseFoldUTF8('Hello World', 'conservative');
SELECT caseFoldUTF8('Straße', 'conservative');
SELECT caseFoldUTF8('HÉLLO', 'conservative');
SELECT caseFoldUTF8('ﬃ', 'conservative'); -- case folding decomposes ffi ligature even in conservative mode
SELECT caseFoldUTF8('Ⅷ', 'conservative'); -- but Roman numeral stays intact as a single character

-- accentFoldUTF8: diacritic removal
SELECT '-- accentFoldUTF8';
SELECT accentFoldUTF8('café résumé naïve');
SELECT accentFoldUTF8('Ångström');
SELECT accentFoldUTF8('piñata');

-- foldUTF8: combined case + accent folding
SELECT '-- foldUTF8 aggressive (default)';
SELECT foldUTF8('Café Résumé');
SELECT foldUTF8('HÉLLO Wörld');
SELECT foldUTF8('Straße');

SELECT foldUTF8('Ǆeﬃcient') AS aggressive, foldUTF8('Ǆeﬃcient', 'conservative') AS conservative; -- compatibility chars: aggressive decomposes both, conservative only decomposes ligature

SELECT '-- foldUTF8 conservative';
SELECT foldUTF8('Café Résumé', 'conservative');
SELECT foldUTF8('HÉLLO Wörld', 'conservative');

-- caseFoldUTF8 with handle_turkic_i
SELECT '-- caseFoldUTF8 with special I handling';
SELECT caseFoldUTF8('İstanbul', 'conservative', 0);
SELECT caseFoldUTF8('İstanbul', 'conservative', 1);

-- Empty string
SELECT '-- empty strings';
SELECT caseFoldUTF8('');
SELECT accentFoldUTF8('');
SELECT foldUTF8('');

-- Single character inputs
SELECT '-- single chars';
SELECT caseFoldUTF8('A'), caseFoldUTF8('a'), caseFoldUTF8('é'), caseFoldUTF8('Ω');
SELECT accentFoldUTF8('A'), accentFoldUTF8('a'), accentFoldUTF8('é'), accentFoldUTF8('Ω');
SELECT foldUTF8('A'), foldUTF8('a'), foldUTF8('é'), foldUTF8('Ω');

-- ASCII-only (no-op for accent fold)
SELECT '-- ASCII only';
SELECT caseFoldUTF8('ABC');
SELECT caseFoldUTF8('abc');
SELECT accentFoldUTF8('abc');
SELECT foldUTF8('ABC');
SELECT foldUTF8('abc');

-- Supplementary plane characters (surrogate pairs in UTF-16)
SELECT '-- supplementary plane';
SELECT caseFoldUTF8('𝐀𝐁𝐂');
SELECT accentFoldUTF8('𝐀𝐁𝐂');
SELECT foldUTF8('𝐀𝐁𝐂');
SELECT caseFoldUTF8('Hello 🌍');

-- String of only combining marks (should produce empty string)
SELECT '-- only combining marks';
SELECT accentFoldUTF8(char(0xCC, 0x81, 0xCC, 0x88));

-- Multiple accents on one base character
SELECT '-- multiple accents';
SELECT accentFoldUTF8('ạ̈');

-- CJK passthrough (no case, no accents)
SELECT '-- CJK passthrough';
SELECT caseFoldUTF8('日本語テスト'), accentFoldUTF8('日本語テスト'), foldUTF8('日本語テスト');

-- foldUTF8 with handle_turkic_i (plain I becomes dotless ı)
SELECT '-- foldUTF8 exclude_special_I';
SELECT foldUTF8('DİYARBAKIR', 'conservative', 0);
SELECT foldUTF8('DİYARBAKIR', 'conservative', 1);

-- Idempotency: applying twice gives the same result
SELECT '-- idempotency';
SELECT foldUTF8(foldUTF8('Café Résumé')) = foldUTF8('Café Résumé');
SELECT caseFoldUTF8(caseFoldUTF8('Straße')) = caseFoldUTF8('Straße');
SELECT accentFoldUTF8(accentFoldUTF8('piñata')) = accentFoldUTF8('piñata');

-- Multi-row table test
SELECT '-- table test';
DROP TABLE IF EXISTS test_fold_utf8;
CREATE TABLE test_fold_utf8 (s String) ENGINE = Memory;
INSERT INTO test_fold_utf8 VALUES ('Hello World'), ('Straße'), ('HÉLLO'), ('café résumé'), ('ﬃ'), ('');
SELECT s, caseFoldUTF8(s), accentFoldUTF8(s), foldUTF8(s) FROM test_fold_utf8 ORDER BY s;
DROP TABLE test_fold_utf8;
