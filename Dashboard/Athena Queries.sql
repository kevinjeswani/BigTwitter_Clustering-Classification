
--**************************************************************************************************************************
--Creaing the table from S3 bucket raw data before Machine Learning that was cleaned in DataBricks

CREATE EXTERNAL TABLE IF NOT EXISTS `bigdataproject_db`.`TwitterInflation_clean` (
  `id` string,
  `name` string,
  `username` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string,
  `tweet_clean` string,
  `f_retweet` string,
  `f_reply` string,
  `f_mentions` string,
  `f_hastag` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ptb2-junaid/twitterInflation_clean.csv/'
TBLPROPERTIES ('classification' = 'csv');


--Creating an updated table with CTAS method for cleaning data. Below is the methodology (steps) 
-- 1) No baddata due to Delimiter present in Username and/or Tweets
-- 2) Date extracted out of the created_at column
-- 3) Cleaning Location data at Country level
-- 4) Dropping unnecessary columns

CREATE TABLE twitterinflation_clean_updated AS 
Select 
 id
,username
,tweet
,followers_count
,SUBSTR(created_at,5,6) || ', ' || SUBSTR(created_at,27,4) as create_date --Step #2
,f_reply as tweet_as_reply
,f_mentions as tweet_has_mention
,f_hastag as tweet_has_hashtag
,CASE 
When location like '%USA' Then 'United States'
When location like '%UK' Then 'United Kingdom'
When location like '%London%' Then 'United Kingdom'
When location like '%, NY' Then 'United States'
When location like '%India%' Then 'India'
When location like '%Pakistan%' Then 'Pakistan'
When location like '%England%' Then 'United Kingdom'
When location like '%United Kingdom%' Then 'United Kingdom'
When location like '%Canada%' Then 'Canada'
When location like '%New York%' Then 'United States'
When location like '%Washington%' Then 'United States'
When location like '%Toronto%' Then 'Canada'
When location like 'None' Then 'Earth'
ELSE location
END AS location_updated -- Step #3
from twitterinflation_clean 
WHERE geo LIKE 'None' -- Step #1
and created_at NOT LIKE 'None' -- Step #1 

--****************************************************************************************************************************************************


--Linear Regression ML Model
--Creating post Machine Learning Table from s3 data

CREATE EXTERNAL TABLE IF NOT EXISTS `bigdataproject_db`.`machinelearninglr` (
  `id` string,
  `name` string,
  `username` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `created_at` string,
  `text` string,
  `f_retweet` string,
  `f_reply` string,
  `f_mentions` string,
  `f_hashtag` string,
  `vader_score` int,
  `vader_label` int,
  `snlp_sentiment` string,
  `label` int,
  `predictions` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ptb2-junaid/twitterInflation_lr_pred_all_delimited.csv/'
TBLPROPERTIES ('classification' = 'csv');



--Creating an updated table with CTAS method for cleaning data. Below is the methodology (steps) 
-- 1) No baddata due to Delimiter present in Username and/or Tweets
-- 2) Date extracted out of the created_at column
-- 3) Cleaning location data at country level
-- 4) Dropping unnecessary columns
-- 5) Creating additional column to check the prediction accuracy
-- 6) Creating additonal column for predicted sentiment


CREATE TABLE machinelearninglr_updated AS 
Select 
 id
,username
,tweet
,followers_count
,SUBSTR(created_at,5,6) || ', ' || SUBSTR(created_at,27,4) as create_date --Step #2
,f_reply as tweet_as_reply
,f_mentions as tweet_has_mention
,f_hashtag as tweet_has_hashtag
,snlp_sentiment as sentiment
,CASE 
WHEN Predictions = 0 THEN 'Negative'
WHEN Predictions = 1 THEN 'Neutral'
WHEN Predictions = 2 THEN 'Positive'
END AS prediction_sentiment -- Step #6
,CASE
WHEN predictions = label then 'Accurate'
ELSE 'Inaccurate'
END as Prediction_Accuracy -- Step #5
,CASE 
When location like '%USA' Then 'United States'
When location like '%UK' Then 'United Kingdom'
When location like '%London%' Then 'United Kingdom'
When location like '%, NY' Then 'United States'
When location like '%India' Then 'India'
When location like '%Pakistan' Then 'Pakistan'
When location like '%England%' Then 'United Kingdom'
When location like '%United Kingdom%' Then 'United Kingdom'
When location like '%Canada%' Then 'Canada'
When location like '%New York%' Then 'United States'
When location like '%Washington%' Then 'United States'
When location like '%Toronto%' Then 'Canada'
When location like 'None' Then 'Earth'
ELSE location
END AS location_updated -- Step #3
from "bigdataproject_db"."machinelearninglr" 
where 1=1
and f_retweet LIKE 'false' -- Step #1

--****************************************************************************************************

--Random Forset ML Model
--Creating post Machine Learning Table from s3 data

CREATE EXTERNAL TABLE IF NOT EXISTS `bigdataproject_db`.`machinelearningrf` (
  `id` string,
  `name` string,
  `username` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `created_at` string,
  `text` string,
  `f_retweet` string,
  `f_reply` string,
  `f_mentions` string,
  `f_hashtag` string,
  `vader_score` int,
  `vader_label` int,
  `snlp_sentiment` string,
  `label` int,
  `predictions` int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://ptb2-junaid/twitterInflation_rf_pred_all_delimited.csv/'
TBLPROPERTIES ('classification' = 'csv');


--Creating an updated table with CTAS method for cleaning data. Below is the methodology (steps) 
-- 1) No baddata due to Delimiter present in Username and/or Tweets
-- 2) Date extracted out of the created_at column
-- 3) Cleaning location data at country level
-- 4) Dropping unnecessary columns
-- 5) Creating additional column to check the prediction accuracy
-- 6) Creating additonal column for predicted sentiment

CREATE TABLE machinelearningrf_updated AS 
Select 
 id
,username
,tweet
,followers_count
,SUBSTR(created_at,5,6) || ', ' || SUBSTR(created_at,27,4) as create_date --Step #2
,f_reply as tweet_as_reply
,f_mentions as tweet_has_mention
,f_hashtag as tweet_has_hashtag
,snlp_sentiment as sentiment
,CASE 
WHEN Predictons = 0 THEN 'Negative'
WHEN Predictons = 1 THEN 'Neutral'
WHEN Predictons = 2 THEN 'Positive'
END AS prediction_sentiment -- Step #6
,CASE
WHEN predictons = label then 'Accurate'
ELSE 'Inaccurate'
END as Prediction_Accuracy -- Step #5
,CASE 
When location like '%USA' Then 'United States'
When location like '%UK' Then 'United Kingdom'
When location like '%London%' Then 'United Kingdom'
When location like '%, NY' Then 'United States'
When location like '%India' Then 'India'
When location like '%Pakistan' Then 'Pakistan'
When location like '%England%' Then 'United Kingdom'
When location like '%United Kingdom%' Then 'United Kingdom'
When location like '%Canada%' Then 'Canada'
When location like '%New York%' Then 'United States'
When location like '%Washington%' Then 'United States'
When location like '%Toronto%' Then 'Canada'
When location like 'None' Then 'Earth'
ELSE location
END AS location_updated -- Step #3
from "bigdataproject_db"."machinelearningrf" 
where 1=1
and f_retweet LIKE 'false' -- Step #1




--*******************************************************************************************************************

--SQL Analysis section

--Pre machine learning data analysis queries

--Checking the total number of rows in the updated table
Select Count(*) as "Tweet_count"
from twitterinflation_clean_updated;

--Checking the Top followed users who tweeted about Inflation
Select DISTINCT(username),AVG(followers_count) as"Followers Count" 
from twitterinflation_clean_updated 
Group by username 
Order by 2 desc 



--Post Machine Learning data analysis queries (Only for Liner Regression)

-- Querying for actual vs predicted sentiments

Select count(*) as "Tweet_count",label
from "bigdataproject_db"."machinelearninglr_updated" 
where 1=1
and f_retweet LIKE 'false'
group by label

Select count(*) as "Tweet_count",predictons 
from "bigdataproject_db"."machinelearninglr_updated" 
where 1=1
and f_retweet LIKE 'false'
group by predictons


--Querying for most sentiments (orignial model) by country
Select Count(*) as "Tweet_count",sentiment,location_updated
from "bigdataproject_db"."machinelearninglr_updated" 
where 1=1
Group by sentiment,location_updated
order by 1 desc
Limit 100


--Querying for most sentiments (trained model) by country
Select Count(*) as "Tweet_count",prediction_sentiment,location_updated
from "bigdataproject_db"."machinelearninglr_updated" 
where 1=1
and location_updated NOT LIKE 'Earth'
Group by prediction_sentiment,location_updated
order by 1 desc
Limit 100




