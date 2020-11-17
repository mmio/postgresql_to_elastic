\i ./elastic_status_commands.sql

SELECT get_product(100)->'_source'->'name';
SELECT get_health();
