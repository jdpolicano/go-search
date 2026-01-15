-- Database cleanup script for go-search engine
-- Truncates all tables in the public schema while preserving table structures
DO
$$
DECLARE
    table_name text;
BEGIN
    FOR table_name IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('TRUNCATE TABLE %I CASCADE', table_name);
    END LOOP;
END;
$$;
