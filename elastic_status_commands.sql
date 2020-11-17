\i environment.sql

CREATE OR REPLACE FUNCTION build_request(endpoint TEXT)
RETURNS HTTP_RESPONSE
AS
$$
	BEGIN
		RETURN http_get(
			format('%s:%s%s',
				current_setting('ELASTIC.IP'),
				current_setting('ELASTIC.PORT'),
				endpoint
			)
		);
	END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_health()
RETURNS HTTP_RESPONSE
AS
$$
	BEGIN
		RETURN build_request('/_cat/health?v');
	END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_product(product_id bigint)
RETURNS JSON
AS
$$
        BEGIN
                RETURN content FROM build_request(format('/products/_doc/%s', product_id));
        END;
$$
LANGUAGE plpgsql;
