\i conf.sql

-- Bulk
DROP FUNCTION IF EXISTS es_send_bulk(batch TEXT);
CREATE OR REPLACE FUNCTION es_send_bulk(batch TEXT)
RETURNS HTTP_RESPONSE
LANGUAGE plpgsql
AS $$
	BEGIN
		RETURN http_post(
			format(
				'%s:%s/_bulk',
				current_setting('ELASTIC.IP'),
				current_setting('ELASTIC.PORT')),
			batch,
			'application/x-ndjson');
	END;
$$;

DROP FUNCTION IF EXISTS es_add_metadata(value TEXT, index TEXT);
CREATE FUNCTION es_add_metadata(value TEXT, index TEXT)
RETURNS TEXT
LANGUAGE SQL
AS $$
   SELECT
   		E'{ "create": { "_index": "' || index || E'" } }\n' ||
		value ||
		E'\n';
$$;

-- Error handling
DROP FUNCTION IF EXISTS es_bad_response(response HTTP_RESPONSE);
CREATE FUNCTION es_bad_response(response HTTP_RESPONSE)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $$
   BEGIN
		IF (response.status)::TEXT LIKE '200' THEN
      		RETURN FALSE;
   		END IF;

   		RETURN TRUE;
   END;
$$;

DROP PROCEDURE IF EXISTS es_show_res_error(response HTTP_RESPONSE);
CREATE PROCEDURE es_show_res_error(response HTTP_RESPONSE)
LANGUAGE PLPGSQL
AS $$
	BEGIN
		RAISE EXCEPTION 'Bad elastic response: %, msg: %', response.status, response.content
   	 	USING HINT = 'Fix the error then rerun the insert script';
	END;
$$;

DROP FUNCTION IF EXISTS es_bulk_has_errors(response HTTP_RESPONSE);
CREATE FUNCTION es_bulk_has_errors(response HTTP_RESPONSE)
RETURNS BOOLEAN
LANGUAGE PLPGSQL
AS $$
	BEGIN
   		IF ((response.content::json)->'errors')::TEXT LIKE 'true' THEN
      		RETURN TRUE;
   		END IF;

   		RETURN FALSE;
   	END;
$$;

DROP FUNCTION IF EXISTS es_bulk_get_error_items(response HTTP_RESPONSE);
CREATE FUNCTION es_bulk_get_error_items(response HTTP_RESPONSE)
RETURNS TABLE (json JSON)
LANGUAGE PLPGSQL
AS $$
	BEGIN
   		RETURN QUERY
			SELECT j
			FROM json_array_elements((response.content::json)->'items') as j
			WHERE (j->'index'->'status')::TEXT NOT LIKE '201';
	END;
$$;

DROP PROCEDURE IF EXISTS es_bulkitem_get_error(item RECORD);
CREATE PROCEDURE es_bulkitem_get_error(item RECORD)
LANGUAGE PLPGSQL
AS $$
	BEGIN
		RAISE NOTICE 'Error creating item: %', item.json->'index'->'error';
	END;
$$;
