\i conf.sql
\i elastic_api.sql
\i convert_to_json.sql

-- DROP unneeded temporary tables
DROP TABLE IF EXISTS parentless_tweets_json;
DROP TABLE IF EXISTS accounts_by_mention_json;
DROP TABLE IF EXISTS hashtags_by_mention_json;
DROP TABLE IF EXISTS accounts_json;

-- BULK INSERT FROM TWEETS
DROP TABLE IF EXISTS tmp;
CREATE UNLOGGED TABLE tmp
(
    request TEXT
);

DO
$$
    DECLARE
        tweet_count	BIGINT = (SELECT max(id) FROM tweets_json);
        batch_size 	INT    = 100;
    BEGIN
        FOR iter IN 0..tweet_count BY batch_size
            LOOP
		INSERT INTO tmp (request)
		       SELECT
				string_agg(
				  jsn::TEXT,
                                  E'\n' || '{ "index": { "_index": "tweets" } }' || E'\n')
                       FROM (
		       	    SELECT jsn
                            FROM tweets_json
			    WHERE id >= iter
			    LIMIT batch_size) AS j;
		RAISE NOTICE 'Requests created % of % items.', iter + batch_size, tweet_count;
            END LOOP;
	    UPDATE tmp SET request = '{ "index": { "_index": "tweets" } }' || E'\n' || request || E'\n';
    END;
$$;

DO
$$
    DECLARE
        req TEXT;
	max INT = (select count(*) from tmp);
	cur INT = 0;
	sts TEXT;
    BEGIN
        FOR req IN SELECT request FROM tmp
            LOOP
		cur := cur + 1;

		sts := (SELECT status
                     FROM http_post(
                             '172.17.0.1:9200/_bulk',
                             req,
                             'application/x-ndjson'));

		if sts != '200' then
		   RAISE NOTICE '%/%', cur, max;
		   RAISE NOTICE '%', sts;
		end if;

            END LOOP;
    END;
$$;

DROP TABLE IF EXISTS tweets_json;
DROP TABLE IF EXISTS tmp;
