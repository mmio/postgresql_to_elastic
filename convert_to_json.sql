-- (aid, accounts json)
DROP TABLE IF EXISTS accounts_json;

CREATE TABLE accounts_json(id  bigint primary key, jsn JSON);

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

-- (twid, uniq json_hashtags array)
DROP TABLE IF EXISTS hashtags_by_mention_json;

CREATE TABLE hashtags_by_mention_json(tweet_id varchar, jsn json);

INSERT INTO hashtags_by_mention_json
       SELECT th.tweet_id,
       	      json_agg(DISTINCT h.value)
       FROM tweet_hashtags th
       JOIN hashtags h on th.hashtag_id = h.id
       GROUP BY th.tweet_id;

-- (twid, accounts which mention the tweet)
DROP TABLE accounts_by_mention_json;

CREATE TABLE accounts_by_mention_json(tweet_id varchar, jsn json);

INSERT INTO accounts_by_mention_json
SELECT tm.tweet_id,
       json_agg(a.jsn)
  FROM tweet_mentions tm
       JOIN accounts_json a on tm.account_id = a.id
 GROUP BY tm.tweet_id;

-- Parentless Tweets
DROP TABLE parentless_tweets_json;

CREATE TABLE parentless_tweets_json
(
    tweet_id varchar,
    jsn      json
);

INSERT INTO parentless_tweets_json
SELECT t.id,
	json_strip_nulls(
		json_build_object(
		        'content', replace(t.content, '\n', ' '),
		        'author', aj.jsn,
		        'hashtags', hbmj.jsn,
		        'mentions', abmj.jsn,
			'location', ST_AsGeoJSON(ST_AsText(t.location)),
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

-- Tweets with parents
DROP TABLE IF EXISTS tweets_json;

CREATE TABLE tweets_json
(
    id SERIAL PRIMARY KEY,	-- For effective pagination
    jsn json
);

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
