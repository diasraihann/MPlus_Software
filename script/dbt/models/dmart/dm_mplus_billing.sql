{{ config(
    tags=["dm_mplus_billing"],
    materialized='table'
) }}
WITH last_stt AS (
SELECT 
    "number"
    , "date"
    , client_code
    , amount
    , client_type
    , source_file
    , ROW_NUMBER()OVER(PARTITION BY "number" ORDER BY source_file DESC) AS rn
    , insertdate
FROM refine.ref_mplus_stt s
) 
, counting_stt AS (
	SELECT 
	   "date"
	   , client_code
		, client_type
		, COUNT(*) AS stt_count
	FROM last_stt
	WHERE rn = 1
	GROUP BY "date", client_code, client_type
)
SELECT  
	s."date"
	, s.client_code
	, c.stt_count
	, CASE
			WHEN s.client_type = 'C' THEN SUM(s.amount)
			ELSE NULL
		END AS debit
	, CASE
			WHEN s.client_type = 'V' THEN SUM(s.amount)
			ELSE NULL
		END AS credit
	, s.insertdate
	, CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS updatedate
FROM last_stt s
LEFT JOIN counting_stt c ON s."date" = c."date" 
    AND s.client_code = c.client_code 
    AND s.client_type = c.client_type
WHERE s.rn = 1
GROUP BY
	s."date"
	, s.client_code
	, c.stt_count
	, s.client_type
	, s.insertdate
ORDER BY 
	s."date"
	, s.client_code