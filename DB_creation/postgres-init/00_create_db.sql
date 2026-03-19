-- Create sandbox database for Ava if it does not exist.
SELECT 'CREATE DATABASE ava_sandbox'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ava_sandbox')\gexec
