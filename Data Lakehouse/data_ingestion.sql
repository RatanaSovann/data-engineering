--Create & Use Database
CREATE DATABASE assignment_1;
USE DATABASE assignment_1;

--Create Staging area
CREATE OR REPLACE STAGE stage_assignment
URL='azure://utsdbratana.blob.core.windows.net/assignment1'
CREDENTIALS=(AZURE_SAS_TOKEN='use your own sas token here');

list @stage_assignment;

--Create External Table for YouTube trending
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_trending
WITH LOCATION = @stage_assignment
FILE_FORMAT = (TYPE = CSV)
PATTERN = '.*\.csv';

--View Columns Name 
SELECT *
FROM assignment_1.PUBLIC.ex_table_youtube_trending
LIMIT 1;

--Create Format CSV File
CREATE OR REPLACE FILE FORMAT file_format_csv
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

--Use File Format Specified
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_trending
WITH LOCATION = @stage_assignment
FILE_FORMAT = file_format_csv
PATTERN = '.*\.csv';

--Display the first 10 rows of the table
SELECT *
FROM assignment_1.PUBLIC.ex_table_youtube_trending
LIMIT 10;

--Parse value for each columns
SELECT
value:c1::varchar as video_id,
value:c2::varchar as title,
value:c3::date as published_at,
value:c4::varchar as channel_id,
value:c5::varchar as channel_Title,
value:c6::varchar as categoryid,
value:c7::date as trending_date,
value:c8::int as view_count,
value:c9::int as likes,
value:c10::int as dislikes,
value:c11::int as comment_count,
SPLIT_PART(SPLIT_PART(metadata$filename, '/', -1), '_', 1) AS COUNTRY,
FROM ASSIGNMENT_1.PUBLIC.EX_TABLE_YOUTUBE_TRENDING
LIMIT 10;

--Create youtube_trending_table with the correct data type
CREATE OR REPLACE TABLE table_youtube_trending AS
SELECT
value:c1::varchar as video_id,
value:c2::varchar as title,
value:c3::date as published_at,
value:c4::varchar as channel_id,
value:c5::varchar as channel_Title,
value:c6::varchar as categoryid,
value:c7::date as trending_date,
value:c8::int as view_count,
value:c9::int as likes,
value:c10::int as dislikes,
value:c11::int as comment_count,
SPLIT_PART(SPLIT_PART(metadata$filename, '/', -1), '_', 1) AS COUNTRY,
FROM ASSIGNMENT_1.PUBLIC.EX_TABLE_YOUTUBE_TRENDING;

--View sample data
SELECT * 
FROM table_youtube_trending
LIMIT 10;

--Create External Table for YouTube category
CREATE OR REPLACE EXTERNAL TABLE ex_table_youtube_category
WITH LOCATION = @stage_assignment
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*\.json';

SELECT *
FROM assignment_1.PUBLIC.ex_table_youtube_category
LIMIT 10;

--Parse and cast JSON fields
SELECT
    /*
        Get countrry name from the JSON filename prefix
        metadata$filename is @assignment_1.PUBLIC/ex_table_youtube_category/JP_category_id.json 
        We SPLIT and select the last element after / (-1) which is JP_category_id.json
        Then we select the first item before _ which is the country name (i.e JP)
    */
    SPLIT_PART(SPLIT_PART(metadata$filename, '/', -1), '_', 1) AS COUNTRY,
    item.value:id::STRING AS CATEGORYID,
    item.value:snippet.title::STRING AS CATEGORY_TITLE
FROM assignment_1.PUBLIC.ex_table_youtube_category,
LATERAL FLATTEN(input => $1:items) AS item;

--Create table table_youtube_category
CREATE TABLE table_youtube_category AS
SELECT
    SPLIT_PART(SPLIT_PART(metadata$filename, '/', -1), '_', 1) AS COUNTRY,
    item.value:id::STRING AS CATEGORYID,
    item.value:snippet.title::STRING AS CATEGORY_TITLE
FROM assignment_1.PUBLIC.ex_table_youtube_category,
LATERAL FLATTEN(input => $1:items) AS item;

--Check table_youtube_category table
SELECT *
FROM table_youtube_category;

-- Create a final combined table
CREATE OR REPLACE TABLE table_youtube_final AS
SELECT
    UUID_STRING() AS id,
    t.video_id,
    t.title,
    t.published_at,
    t.channel_id,
    t.channel_title,
    c.categoryid,
    c.category_title,
    t.trending_date,
    t.view_count,
    t.likes,
    t.dislikes,
    t.comment_count,
    t.country
FROM ASSIGNMENT_1.PUBLIC.table_youtube_trending AS t
LEFT JOIN ASSIGNMENT_1.PUBLIC.table_youtube_category AS c
    ON t.country = c.country
    AND t.categoryid = c.categoryid;


SELECT *
FROM table_youtube_final;


