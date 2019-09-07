# Automated Extract Transform Load Pipeline using Amazon Redshift & Apache Airflow

## Source Data

Data about songs and user events listening to them are stored in 2 separate S3 buckets.

* Log events - `s3://sparkify/log_data` 
* Song data - `s3://sparkify/song_data`

The user events log files contain the following information.

![Event logs](https://github.com/command7/SQL_SQL_ETL_Airflow/blob/master/Images/log-data.png)

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

In order to load them in to a data warehouse (*Redshift*), a data model was designed using STAR schema containing fact and dimension tables.

### Fact Table

*_songplays_*

* `songplay_id`
* `start_time`
* `user_id`
* `level`
* `song_id`
* `artist_id`
* `session_id`
* `location`
* `user_agent`

### Dimension Tables

*_users_*

* `user_id`
* `first_name`
* `last_name`
* `gender`
* `level`

*_songs_*

* `song_id`
* `title`
* `artist_id`
* `year`
* `duration`

*_artists_*

* `artist_id`
* `name`
* `location`
* `lattitude`
* `longitude`

*_time_*

* `start_time`
* `hour`
* `day`
* `week`
* `month`
* `year`
* `weekday`


## Pipeline Components

* `S3 bucket` - Unstructured data in the form of logs are stored in separate s3 buckets
* `Redshift` - Data Warehouse used as destination of structured data after ETL process and for staging purposes
* `Airflow` - Automation of ETL pipeline on a daily schedule 

## ETL Process

## How to run

## Configurations required