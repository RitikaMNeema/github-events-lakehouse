-- Iceberg JDBC catalog backing store.
--
-- Kept in a separate database from Airflow metadata so the two can be
-- backed up and reasoned about independently.
CREATE DATABASE iceberg;

\connect iceberg

-- Iceberg's JDBC catalog metadata tables, created up front ON PURPOSE.
--
-- Spark's JdbcCatalog creates these itself on first contact
-- (jdbc.init-catalog-tables defaults to true). Trino's Iceberg connector
-- does NOT -- it expects them to exist and fails every query with
--
--     Cannot check and eventually update SQL schema
--
-- until something else has created them. That produced a silent ordering
-- dependency: Trino only worked if a Spark job had happened to run first,
-- so a cold `make nuke && make up` left dbt broken for reasons that had
-- nothing to do with dbt.
--
-- Creating them here removes the ordering dependency entirely. Both
-- engines then attach to a catalog that already exists.
--
-- This DDL was not written from documentation -- it was captured from
-- information_schema after Iceberg 1.8.1 created the tables itself, so it
-- matches that version exactly. If you bump the Iceberg version and see
-- catalog errors, re-capture it rather than hand-editing.

CREATE TABLE IF NOT EXISTS iceberg_tables (
    catalog_name               VARCHAR(255)  NOT NULL,
    table_namespace            VARCHAR(255)  NOT NULL,
    table_name                 VARCHAR(255)  NOT NULL,
    metadata_location          VARCHAR(1000),
    previous_metadata_location VARCHAR(1000),
    -- Present from the V2 catalog schema; distinguishes tables from views.
    iceberg_type               VARCHAR(5),
    PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
    catalog_name   VARCHAR(255)  NOT NULL,
    namespace      VARCHAR(255)  NOT NULL,
    property_key   VARCHAR(255)  NOT NULL,
    property_value VARCHAR(1000),
    PRIMARY KEY (catalog_name, namespace, property_key)
);
