\i conf.sql
\i elastic_api.sql
\i convert_to_json.sql

-- BULK INSERT FROM TWEETS
DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp(request TEXT);

DO
$$
    DECLARE
	min_id		BIGINT = (SELECT min(id) FROM tweets_json);
        max_id		BIGINT = (SELECT max(id) FROM tweets_json);
        batch_size 	INT    = 1000;
    BEGIN
        FOR iter IN min_id..max_id BY batch_size
	    LOOP
		INSERT INTO tmp (request)
		       SELECT
				E'{ "create": { "_index": "tmp" } }\n' ||
				string_agg(
				  jsn::TEXT, E'\n{ "create": { "_index": "tmp" } }\n') ||
				E'\n'
                       FROM (
		       	    SELECT jsn
                            FROM tweets_json
			    WHERE id >= iter
			    ORDER BY id ASC
			    LIMIT batch_size-1) AS j;

		IF iter + batch_size - 1 > max_id THEN
		   RAISE NOTICE 'Requests created % of % items.', max_id, max_id;
		ELSE
		   RAISE NOTICE 'Requests created % of % items.', iter + batch_size - 1, max_id;
		END IF;
		
            END LOOP;
    END;
$$;

CREATE FUNCTION es_bad_response(response HTTP_RESPONSE)
RETURN BOOLEAN
LANGUAGE PLPGSQL
AS $$
   IF (response.status)::TEXT LIKE '200' THEN
      RETURN FALSE;
   END IF;

   RETURN TRUE;
$$;

CREATE PROCEDURE es_show_res_error(response HTTP_RESPONSE)
LANGUAGE PLPGSQL
AS $$
   RAISE EXCEPTION 'Bad elastic response: %, msg: %', response.status, response.content
   	 USING HINT = 'Fix the error then rerun the insert script';
$$;

CREATE FUNCTION es_bulk_has_errors(response HTTP_RESPONSE)
RETURN BOOLEAN
LANGUAGE PLPGSQL
AS $$
   IF ((response.content::json)->'errors')::TEXT LIKE 'true' THEN
      RETURN TRUE;
   END IF;

   RETURN FALSE;
$$;

CREATE FUNCTION es_bulk_get_error_items(response HTTP_RESPONSE)
RETURN TABLE (json JSON)
LANGUAGE PLPGSQL
AS $$
   RETURN
	SELECT json
	FROM json_array_elements((response.content::json)->'items') as json
	WHERE (json->'index'->'status')::TEXT NOT LIKE '201';
$$;

CREATE PROCEDURE es_bulkitem_get_error(item RECORD)
LANGUAGE PLPGSQL
AS $$
   RAISE NOTICE 'Error creating item: %', item.json->'index'->'error';
$$;

CREATE FUNCTION es_send_bulk(request TEXT)
RETURN HTTP_RESPONSE
LANGUAGE PLPGSQL
AS $$
   RETURN http_post('172.17.0.1:9200/_bulk', request, 'application/x-ndjson');
$$;

DO
$$
    DECLARE
        req TEXT;
	item RECORD;
	res HTTP_RESPONSE;
    BEGIN
        FOR req IN SELECT request FROM tmp ORDER BY LENGTH(request) DESC -- Big batches are more likely to fail
            LOOP
		res := es_send_bulk(req);

		IF es_bad_res(res) THEN
		   es_show_res_error(res);
		END IF;

		IF es_bulk_has_errors(res) THEN
		   FOR item IN es_bulk_get_error_items(res) LOOP
		       es_bulkitem_get_error(item);
		   END LOOP;
		END IF;

            END LOOP;
    END;
$$;


DO
$$
    DECLARE
        req TEXT;
	element RECORD;
	response HTTP_RESPONSE;
    BEGIN
        FOR req IN SELECT request FROM tmp ORDER BY LENGTH(request) DESC
            LOOP
		-- PERFORM pg_sleep(.01);

		response := http_post('172.17.0.1:9200/_bulk', req, 'application/x-ndjson');

		IF (response.status)::TEXT NOT LIKE '200' THEN
		   RAISE NOTICE '%', response.content;
		END IF;

		IF ((response.content::json)->'errors')::TEXT LIKE 'true' THEN
		   FOR element IN SELECT json FROM json_array_elements((response.content::json)->'items') as json
		       LOOP
			IF (element.json->'index'->'status')::TEXT NOT LIKE '201' THEN
			   RAISE NOTICE '%', element.json->'index'->'error';
			END IF;
		       END LOOP;
		END IF;
            END LOOP;
    END;
$$;

DROP TABLE IF EXISTS tweets_json;
DROP TABLE IF EXISTS tmp;

-- Geolocation debug
