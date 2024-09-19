# Introduction

## Project purpose
The purpose of this project is to create a data warehouse for Sparkify, a music streaming service, to analyze user behavior and song preferences. The data warehouse will be designed to store data from various sources, including user activity logs and song metadata.

## Sparkify?
Sparkify is a music streaming service that allows users to listen to songs and create playlists. The service generates a large amount of data, including user activity logs, song metadata, and user information.

## How this Project will Help?
This project will help Sparkify by creating a data warehouse that can store and analyze large amounts of data. The data warehouse will be designed to support business intelligence and data analysis, allowing Sparkify to gain insights into user behavior and song preferences. This will enable Sparkify to make data-driven decisions to improve its services and increase customer satisfaction.

# Project Structure

## Database Schema Design
The database schema design for this project consists of the following tables:
staging_events: A staging table for event data.
staging_songs: A staging table for song data.
songplay: A fact table for songplay data.
users: A dimension table for user data.
songs: A dimension table for song data.
artists: A dimension table for artist data.
time: A dimension table for time data.

## List of files on project
The project consists of four python scripts and a configuration file:
- `create_tables.py`: This script creates the tables in the Redshift database.
- `etl.py`: This script loads the data from S3 into the staging tables in Redshift, and then processes the data into the analytics tables.
- `sql_queries.py`: This script contains all the SQL queries used in the project.
- `dwh.cfg`: This file contains the configuration parameters for the project. The parameters are read from this file by the python scripts.

## ETL Process
The ETL (Extract, Transform, Load) process for this project involves the following steps:

Extract: Data is extracted from various sources, including user activity logs and song metadata.
Transform: Data is transformed into a format suitable for analysis.
Load: Data is loaded into the data warehouse.
The ETL process is implemented using Python.

## Configuration Parameters
The following configuration parameters are required:

- `HOST`: The hostname of the Redshift cluster.
- `DB_NAME`: The name of the Redshift database.
- `USER`: The username to use when connecting to the Redshift cluster.
- `PASSWORD`: The password to use when connecting to the Redshift cluster.
- `PORT`: The port number to use when connecting to the Redshift cluster.
- `DWH_ROLE_ARN`: The ARN of the IAM role that has access to the S3 buckets and the Redshift cluster.
- `LOG_DATA`: The S3 bucket containing the log data.
- `LOG_JSONPATH`: The S3 bucket containing the JSON path file for the log data.
- `SONG_DATA`: The S3 bucket containing the song data.

# Running
To run the project, execute the following commands in order:

- `python create_tables.py`
- `python etl.py`

The first command creates the tables in the Redshift database. The second command loads the data from S3 into the staging tables in Redshift, and then processes the data into the analytics tables.
