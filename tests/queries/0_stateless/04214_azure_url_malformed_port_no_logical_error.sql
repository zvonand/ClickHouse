-- Tags: no-fasttest
-- Tag no-fasttest: needs USE_AZURE_BLOB_STORAGE.
--
-- Regression test for STID 2508-34fb (stoi sub-variant): server abort with
-- "Logical error: 'std::exception. Code: 1001, type: std::invalid_argument,
-- e.what() = stoi: no conversion'" when an Azure connection string contains a
-- BlobEndpoint URL whose port substring is empty or non-numeric.
--
-- Root cause: contrib/azure/sdk/core/azure-core/src/http/url.cpp:49 calls
-- std::stoi on the port substring without validating it is non-empty. ClickHouse
-- then re-throws this std::invalid_argument up to executeQueryImpl, where
-- getCurrentExceptionMessageAndPattern catches std::logic_error and calls
-- abortOnFailedAssertion in debug/sanitizer builds (Common/Exception.cpp:522).
--
-- Fix: AzureBlobStorageCommon.cpp now translates std::logic_error subtypes
-- raised by the Azure SDK into a DB::Exception with BAD_ARGUMENTS so the
-- query returns a clean error instead of aborting the server.

-- Non-numeric port in BlobEndpoint -> Azure SDK std::stoi("abc") -> invalid_argument
SELECT * FROM azureBlobStorage(
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:abc/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1; -- { serverError BAD_ARGUMENTS }

-- Empty port substring (':' followed immediately by '/') -> std::stoi("") -> invalid_argument
SELECT * FROM azureBlobStorage(
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1; -- { serverError BAD_ARGUMENTS }

-- Port number overflow (> uint16 max) -> Azure SDK std::out_of_range
SELECT * FROM azureBlobStorage(
    'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://devstoreaccount1.blob.core.windows.net:99999999999999999999/;',
    'cont',
    'p',
    'CSV')
SETTINGS max_threads = 1; -- { serverError BAD_ARGUMENTS }

SELECT 'ok';
