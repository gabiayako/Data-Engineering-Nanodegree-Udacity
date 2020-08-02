# Introduction

This project, proposed on Udacity's Data Engineering Nanodegree Program, consists in create a star schema optimized for queries on song play analysis for a fictional company called Sparkify. The star schema helps their analytical goals assuming there are not a huge amount of data, therefore, it is possible to make joins among tables without performance issues.

# General Info

### Database Schema Design

The star schema has advantages for analytical purposes such as query performance and also it is easier to understand having each entity separated in a dimension table.

### ETL Process

It was given two kinds of files residing on S3's `udacity-dend` bucket:
- Inside a folder called `song_data` there are json files containing informations about songs and artists. Using the function `process_song_data` on `etl.py` script, I copied data from these json files to a S3 bucket created by me called `udacity-sparkify-project`.

- Inside another folder called `log_data` there are json files with records of events a user can perform, also, there are information about the user and when the event happened. Using the same `process_log_data`, I also copied data from these json files to S3's `udacity-sparkify-project` bucket.

The data from the first staging table was organized in two dimensional tables: `songs` and `artists`. The data from the second was broke in other two dimensional tables: `users` and `time`. The fact table was created selecting data from `log_data` and `songs`, centralizing information as it should be.

# Running scripts

### etc.py
This script copies data from S3 and after some processing, it creates parquet files on S3. It is possible to run it typing on terminal 
```
python3 etl.py
```
