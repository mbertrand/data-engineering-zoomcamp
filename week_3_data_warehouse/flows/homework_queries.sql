-- Question 1
--Create an external table using the fhv 2019 data.
--Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). 
--What is the count for fhv vehicle records for year 2019?

CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-375719.FHV.fhv_2019_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-mb/data/fhv/fhv_tripdata_2019*.csv.gz']
);

CREATE OR REPLACE TABLE taxi-rides-ny-375719.FHV.fhv_2019_nonpartitioned AS
SELECT * FROM taxi-rides-ny-375719.FHV.fhv_2019_external;

SELECT count(*) FROM `taxi-rides-ny-375719.FHV.fhv_2019_external`;


-- Question 2
--Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT count(*) from (SELECT DISTINCT dispatching_base_num from taxi-rides-ny-375719.FHV.fhv_2019_external);
SELECT count(*) from (SELECT DISTINCT dispatching_base_num from taxi-rides-ny-375719.FHV.fhv_2019_nonpartitioned);

--Question 3
-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

SELECT count(*) from taxi-rides-ny-375719.FHV.fhv_2019_nonpartitioned WHERE PUlocationID is null AND DOlocationID is null;

-- Qurstion 4
-- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

-- Question 5
--Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between --pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
--Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the --partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which --most closely matches.

CREATE OR REPLACE TABLE taxi-rides-ny-375719.FHV.fhv_2019_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM taxi-rides-ny-375719.FHV.fhv_2019_nonpartitioned;


SELECT DISTINCT dispatching_base_num from taxi-rides-ny-375719.FHV.fhv_2019_nonpartitioned
WHERE pickup_datetime >= '2019-03-01' and pickup_datetime <= '2019-03-31';


SELECT DISTINCT dispatching_base_num from taxi-rides-ny-375719.FHV.fhv_2019_partitoned_clustered
WHERE pickup_datetime >= '2019-03-01' and pickup_datetime <= '2019-03-31';


-------------

SELECT table_name, partition_id, total_rows
FROM `FHV.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'fhv_2019_partitoned_clustered'
ORDER BY total_rows DESC;