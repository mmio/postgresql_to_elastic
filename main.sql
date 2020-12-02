\i utils.sql
\i elastic_api.sql
\i convert_to_json.sql

DO
$$
    DECLARE
        req TEXT;
		item RECORD;
		res HTTP_RESPONSE;
    BEGIN
        FOR req IN SELECT * FROM create_request_batches('tweets_raw', 'tweets_json') LOOP
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
