## Introduction

This project has the purpose to extract data about events and songs at Sparkify from JSON files and load it into Redshift tables.

## Tables

There are 5 tables. 

Four dimensions tables: 

- users
- songs
- artists
-  time 

And one fact table: 

- songplays

## Process

Firstly, the data is loaded into two staging tables into redshift, then, the data are extracted from these tables to fact or dimensional tables. At the end of the process, there are some data quality checks to certify the data was correctly processed.

![Process](C:\Users\lucas\Desktop\airflow_project\process.png)