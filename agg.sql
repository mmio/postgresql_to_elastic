DROP AGGREGATE IF EXISTS newline_sep_agg(text);
DROP FUNCTION IF EXISTS newline_sep_agg_transfn(acc text, str text);
CREATE FUNCTION newline_sep_agg_transfn(acc text, str text)
    RETURNS text AS 
    $$
        BEGIN
            IF acc IS NULL THEN
                RETURN '{ "index": { "_index": "tweets" } }' || E'\n' || str;
            ELSE
                RETURN acc || E'\n' || '{ "index": { "_index": "tweets" } }' || E'\n' || str;
            END IF;
        END;
    $$
    LANGUAGE plpgsql IMMUTABLE;

DROP FUNCTION IF EXISTS newline_sep_agg_final(acc text);
CREATE FUNCTION newline_sep_agg_final(acc text)
    RETURNS text AS 
    $$
        BEGIN
		RETURN acc || E'\n';
        END;
    $$
    LANGUAGE plpgsql IMMUTABLE;

CREATE AGGREGATE newline_sep_agg(text) (
    SFUNC=newline_sep_agg_transfn,
    FINALFUNC=newline_sep_agg_final,
    STYPE=text
);
