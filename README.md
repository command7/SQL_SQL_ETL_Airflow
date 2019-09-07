# Automated Extract Transform Load Pipeline using Amazon Redshift & Apache Airflow

## Source Data

Data about songs and user events listening to them are stored in 2 separate S3 buckets.

* Log events - `s3://sparkify/log_data` 
* Song data - `s3://sparkify/song_data`

The user events log files contain the following information.

* 

The songs log files contain the following information

* `num_songs`
* `artist_id`
* `artist_latitude`
* `artist_longitude`
* `artist_location`
* `artist_name`
* `song_id`
* `title`
* `duration` 
* `year`

## STAR Schema Design

![Event logs](https://github.com/command7/SQL_SQL_ETL_Airflow/blob/master/Images/log-data.png)

## Pipeline Components

## ETL Process

## How to run

## Configurations required