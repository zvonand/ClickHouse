-- Tags: no-darwin

-- /proc/version always exists on Linux and contains a non-empty string.
SELECT length(line) > 0 FROM file('/proc/version', 'LineAsString') LIMIT 1;
