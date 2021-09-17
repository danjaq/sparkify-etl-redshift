# Data Warehouse
An ETL pipeline to analyze the listening behavior of Sparkify's users, built for Udactiy's Data Engineering Nanodegree. 

## Overview
This ETL pipeline combines two data sets: 1) a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) and 2) simulated user logs from a music streaming service. Both datasets are stored on S3 as JSON files. The data is read from there onto staging tables within Redshift and from there inserted into a set of final tables. These tables use a star schema for the benefit of simplifying the queries needed. The fact table is `songplays` and the following are the dimension tables: `time`, `users`, `songs`, and `artists`.

## Files
- create_tables.py : Python script to drop and create the tables anew.
- etl.py : Python script to create the full pipeline.
- README.md: This file/
- sql_queries.py: Python script containing a list of queries to create, drop, and query the database.
- dwh.cfg : Config file for Redshift data warehouse.

## Run Instructions
1. Edit dwh.cfg appropriately for your cluster.
2. Run `create_tables.py` to create the needed tables for import. 
3. Run `etl.py` to import the data from S3 JSON files.
4. Query as needed.

### Cluster Settings
For testing purposes, a dc2.large cluster with 2 nodes was created on US-West-2. An IAM role with AmazonS3ReadOnlyAccess was used, and default settings were used for everything except for making the cluster publically accessible.

### Sample Queries 
Please find some sample queries below along with their output.

#### Top 5 Songs
`SELECT COUNT(DISTINCT sp.songplay_id) AS "Total Plays",
        s.title
FROM songplays AS sp
JOIN songs AS s
    ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY "Total Plays" DESC
LIMIT 5;`
            
| totalplays | title |
|------------|-------|
| 37         | You're The One |
| 9          | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) |
| 9          | I CAN'T GET STARTED |
| 8          | Nothin' On You \[feat. Bruno Mars\] (Album Version) |
| 6          | Hey Daddy (Daddy's Home) |

#### Gender Breakdown of Users
`SELECT COUNT(user_id) AS "Total",
        gender
FROM users
GROUP BY gender
ORDER BY "Total" DESC;`
            
| total  | gender |
|--------|--------|
| 60     | F      |
| 45     | M      |