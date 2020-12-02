-- Helper functions
DROP PROCEDURE IF EXISTS drop_tmp_json_tables();
CREATE PROCEDURE drop_tmp_json_tables()
LANGUAGE SQL
AS $$
   DROP TABLE IF EXISTS
   	parentless_tweets_json,
	accounts_by_mention_json,
	hashtags_by_mention_json,
	accounts_json;
$$;

DROP PROCEDURE IF EXISTS drop_all_json_tables();
CREATE PROCEDURE drop_all_json_tables()
LANGUAGE SQL
AS $$
   CALL drop_tmp_json_tables();
   DROP TABLE IF EXISTS tweets_json;
$$;

DROP PROCEDURE IF EXISTS create_all_json_tables();
CREATE PROCEDURE create_all_json_tables()
LANGUAGE SQL
AS $$
   CALL drop_all_json_tables();
   CREATE TABLE accounts_json(id  bigint primary key, jsn JSON);
   CREATE TABLE hashtags_by_mention_json(tweet_id varchar, jsn json);
   CREATE TABLE accounts_by_mention_json(tweet_id varchar, jsn json);
   CREATE TABLE parentless_tweets_json(tweet_id varchar, jsn JSON);
   CREATE TABLE tweets_json(id SERIAL PRIMARY KEY,	jsn JSON);
$$;

DROP FUNCTION IF EXISTS normalize_text(input TEXT)
RETURN TEXT
LANGUAGE SQL
AS $$
	SELECT replace(trim(both '"' from input), '\n', ' ')
$$;

CALL create_all_json_tables();

-- Convert accounts to json 
INSERT INTO accounts_json (id, jsn)
    SELECT
        id,
        json_strip_nulls(
			json_build_object(
				'screen_name', trim(both '"' from screen_name),
				'name', trim(both '"' from name),
				'description', replace(description, '\n', ' '),
				'followers_count', followers_count,
				'friends_count', friends_count,
				'statuses_count', statuses_count))
    FROM accounts;

-- Aggregate accounts by mentions
INSERT INTO accounts_by_mention_json
	SELECT
		tm.tweet_id,
		json_agg(DISTINCT a.jsn::jsonb)
	FROM tweet_mentions tm
		JOIN accounts_json a on tm.account_id = a.id
	GROUP BY tm.tweet_id;

-- Aggregate hashtags by mentions
INSERT INTO hashtags_by_mention_json
	SELECT
		th.tweet_id,
		json_agg(DISTINCT h.value)
	FROM tweet_hashtags th
		JOIN hashtags h on th.hashtag_id = h.id
	GROUP BY th.tweet_id;

-- Parentless(Non-retweet) Tweets
INSERT INTO parentless_tweets_json
	SELECT t.id,
		json_strip_nulls(
			json_build_object(
				'content', replace(trim(both '"' from t.content), '\n', ' '),
				'author', aj.jsn,
				'hashtags', hbmj.jsn,
				'mentions', abmj.jsn,
				'location', ST_AsGeoJSON(ST_AsText(t.location))::json,
				'retweet_count', t.retweet_count,
				'favorite_count', t.favorite_count,
				'happended_at', t.happended_at,
				'country_code', c.code,
				'country_name', c.name))
	FROM tweets t
			JOIN accounts_by_mention_json abmj on t.id = abmj.tweet_id
			JOIN hashtags_by_mention_json hbmj on t.id = hbmj.tweet_id
			LEFT JOIN countries c on t.country_id = c.id
			LEFT JOIN accounts_json aj on t.author_id = aj.id
	WHERE t.parent_id IS NULL;

-- All Tweets
INSERT INTO tweets_json (jsn)
	SELECT
		json_strip_nulls(
			json_build_object(
				'content', replace(trim(both '"' from t.content), '\n', ' '),
				'author', aj.jsn,
				'parent', ptj.jsn,
				'hashtags', hbmj.jsn,
				'mentions', abmj.jsn,
				'location', ST_AsGeoJSON(ST_AsText(t.location))::json,
				'retweet_count', t.retweet_count,
				'favorite_count', t.favorite_count,
				'happended_at', t.happended_at,
				'country_code', c.code,
				'country_name', c.name))
	FROM tweets t
			LEFT JOIN accounts_by_mention_json abmj on t.id = abmj.tweet_id
			LEFT JOIN hashtags_by_mention_json hbmj on t.id = hbmj.tweet_id
			LEFT JOIN accounts_json aj on t.author_id = aj.id
			LEFT JOIN countries c on t.country_id = c.id
			LEFT JOIN parentless_tweets_json ptj on t.parent_id = ptj.tweet_id;

CALL drop_tmp_json_tables();
