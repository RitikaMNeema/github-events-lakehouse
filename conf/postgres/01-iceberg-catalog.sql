-- Iceberg's JDBC catalog keeps its table pointers here.
-- Separate database from Airflow metadata so the two can be backed up
-- and reasoned about independently.
CREATE DATABASE iceberg;
