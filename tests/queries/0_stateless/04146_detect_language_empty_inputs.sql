SET allow_experimental_nlp_functions = 1;

SELECT detectLanguage('');
SELECT detectLanguage(toString(reinterpretAsUInt8(0)));
SELECT mapSort(detectLanguageMixed(''));
SELECT mapSort(detectLanguageMixed(toString(reinterpretAsUInt8(0))));
