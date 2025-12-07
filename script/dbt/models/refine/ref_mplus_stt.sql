{{ config(
    tags=["mplus_stt", "drive"],
    materialized='table'
) }}
SELECT
    number::varchar(256)
    , date::date
    , client_code::varchar(20)
    , amount::int
    , client_type::varchar(10)
    , source_file::varchar(256)
    , CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta' AS insertdate
FROM refine.mplus_stt
WHERE 
    number IS NOT NULL
    AND date IS NOT NULL
    AND client_code IS NOT NULL 
    AND amount IS NOT NULL 
    AND client_type IS NOT NULL
