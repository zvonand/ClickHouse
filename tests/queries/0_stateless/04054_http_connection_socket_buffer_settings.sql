-- Verify socket buffer settings exist with correct defaults
SELECT name, value, type
FROM system.server_settings
WHERE name IN (
    'disk_connections_rcvbuf',
    'disk_connections_sndbuf',
    'storage_connections_rcvbuf',
    'storage_connections_sndbuf',
    'http_connections_rcvbuf',
    'http_connections_sndbuf'
)
ORDER BY name;
