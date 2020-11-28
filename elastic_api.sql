DROP FUNCTION IF EXIST send_batch(batch TEXT);

-- batch should consist of newline separated json objects
CREATE OR REPLACE FUNCTION send_batch(batch TEXT)
RETURNS HTTP_RESPONSE
AS
$$
	BEGIN
		RETURN http_post(
			format(
				'%s:%s/_bulk',
				current_setting('ELASTIC.IP'),
				current_setting('ELASTIC.PORT')),
			batch,
			'application/x-ndjson');
	END;
$$
LANGUAGE plpgsql;
