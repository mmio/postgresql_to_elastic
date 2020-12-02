\i utils.sql
\i elastic_api.sql
\i convert_to_json.sql

DO
$$
    DECLARE
        req TEXT;
		item RECORD;
		res HTTP_RESPONSE;
		table_with_json_tweets TEXT;
    BEGIN
		table_with_json_tweets := convert_tables_to_json();
		RAISE NOTICE '%', table_with_json_tweets;
        FOR req IN SELECT * FROM create_request_batches('tweets_raw', table_with_json_tweets) LOOP
			EXIT;
			res := es_send_bulk(req);

			IF es_bad_response(res) THEN
		   		CALL es_show_res_error(res);
			ELSIF es_bulk_has_errors(res) THEN
		   		FOR item IN SELECT * FROM es_bulk_get_error_items(res) LOOP
		       		CALL es_bulkitem_get_error(item);
		   		END LOOP;
			END IF;
        END LOOP;
    END;
$$;
