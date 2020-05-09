# Building Cloud Data warehouse with S3 and Redshift 


## Introduction
A music streaming startup has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project involves building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Schema

![Table Schema](/img/pagila-star.png "schema")

## ETL process
In this project most of ETL is done with SQL (Python used just as bridge), transformation and data normalization is done by Query, check out the sql_queries python module

## How to run
Although the data-sources are provided by two S3 buckets the only thing you need for running the example is an ```AWS Redshift Cluster``` up and running

### Creating the Redshift cluster
THis can be done on [link](aws.amazon.com) using the site's GUI tools
* A Redshift dc2.large cluster with 4 nodes has been created, with a cost of USD 0.25/h (on-demand option) per cluster

* We will use ```IAM role``` authorization mechanism, the only policy attached to this IAM will be am AmazonS3ReadOnlyAccess


### Open terminal session and run in the following order

Creating the Postgres tables using the Create tables script. Run ```create_tables.py```
Populating the tables using ETL process script. Run ```etl.py```

## File structure

* /img - folder with images that are used in the README.md
* create_tables.py - This script will drop old tables (if exist) ad re-create the new tables
* etl.py - This script executes the queries that extract JSON data from the S3 bucket, stage and load them to Redshift
* sql_queries.py - This file contains variables with SQL statement in String formats, partitioned by CREATE, DROP, COPY and INSERT statements
dwh.cfg - Configuration file used that contains security info about Redshift, IAM and S3

![Table Schema](/img/pagila-star.png "schema")
