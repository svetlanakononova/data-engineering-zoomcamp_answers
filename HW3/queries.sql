CREATE OR REPLACE EXTERNAL TABLE trip_dataset.fhv_tripdata_2019
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_warm-ring-375016/fhv_tripdata/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE EXTERNAL TABLE trip_dataset.fhv_tripdata_2019_parquet
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_warm-ring-375016/fhv_tripdata/fhv_tripdata_2019-*.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE trip_dataset.fhv_tripdata_2019_non_partitoned AS
SELECT * FROM trip_dataset.fhv_tripdata_2019;
--43,244,696

select count(distinct(affiliated_base_number)) from trip_dataset.fhv_tripdata_2019;
-- 0 MB
select count(distinct(affiliated_base_number)) from trip_dataset.fhv_tripdata_2019_non_partitoned;
-- 317.94MB 

select count(1) from trip_dataset.fhv_tripdata_2019_non_partitoned where PUlocationID is null and DOlocationID is null;
--717748

--Partition by pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE trip_dataset.fhv_tripdata_2019_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS
SELECT * FROM trip_dataset.fhv_tripdata_2019_non_partitoned;

select count(distinct(affiliated_base_number)) from trip_dataset.fhv_tripdata_2019_non_partitoned
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
--647.87 MB

select count(distinct(affiliated_base_number)) from trip_dataset.fhv_tripdata_2019_partitoned_clustered
where date(pickup_datetime) between '2019-03-01' and '2019-03-31';
--23.05MB
