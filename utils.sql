DROP FUNCTION paginate(t TEXT, columns TEXT, start INT, count INT);
CREATE FUNCTION paginate(t TEXT, columns TEXT, start INT, count INT)
RETURNS TABLE(jsn JSON)
LANGUAGE PLPGSQL
AS $$
   BEGIN
      RETURN QUERY
         EXECUTE format(
            'SELECT %s
            FROM %s
            WHERE id >= %s
            ORDER BY id ASC
            LIMIT %s',
            columns, t, start, count);
   END;		
$$;

DROP FUNCTION IF EXISTS last_item_id(start BIGINT, step BIGINT, end BIGINT);
CREATE FUNCTION last_item_id(first BIGINT, step BIGINT, last BIGINT)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
BEGIN
   IF first + step - 1 > last THEN
      RETURN last;
   ELSE
      RETURN first + step - 1;
   END IF;
END;
$$;


DROP FUNCTION create_request_batches(index TEXT, source_table TEXT, batch_size INT);
CREATE FUNCTION create_request_batches(index TEXT, source_table TEXT, batch_size INT DEFAULT 1000)
RETURNS TABLE(request TEXT)
LANGUAGE PLPGSQL
AS $$
   DECLARE
      min_id   BIGINT;
      max_id   BIGINT;
   BEGIN
      EXECUTE format('SELECT min(id), max(id) FROM %s', source_table) INTO min_id, max_id;

      FOR current_id IN min_id..max_id BY batch_size LOOP
         RETURN QUERY
            SELECT string_agg(es_add_metadata(jsn::TEXT, index), '')
            FROM paginate(source_table, 'jsn', current_id, batch_size);

         RAISE NOTICE 'Requests created % of % items.', last_item_id(current_id, batch_size, max_id), max_id;
      END LOOP;
    END;
$$;
