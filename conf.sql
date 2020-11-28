SET ELASTIC.IP TO '172.17.0.1';
SET ELASTIC.PORT TO '9200';

SELECT http_set_curlopt('CURLOPT_TIMEOUT_MS', '100000');
-- SELECT * FROM http_list_curlopt(); -- verification
