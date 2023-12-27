# cancer_dashboard_pipeline

1. Convert the xlsx file into csv with '|' as the separator, as snowflake only accepts csv, tsv for structured formats and JSON, Avro, ORC, Parquet and XML for unstructured data,
2. Create account level and database level objects, including uploading file to the staging area, built schemas for the raw data. These are done using snowflake sql.
3. Create streams on the raw data database for incremental processing and orchestration, then load raw data to the raw database models. these are done using snowpark python.
4. Create stored procedures to transform raw data by consuming the streams to prepare the raw data to fit into star schema database models and create views. these are done with the combination of snowflake sql for the stored procedures and snowpark python for the logic.
5. Create stored procedures to update the fact tables from the views created in the transformation steps. This depends on the transform raw data stored procedures and it won't start unless it completes. these are done with the combination of snowflake sql for the stored procedures and snowpark python for the logic.
6. Create stored procedures to update the dimension tables from the vies created in the transformation steps. This depends on the transform raw data stored procedures and it won't start unless it completes. these are done with the combination of snowflake sql for the stored procedures and snowpark python for the logic.
7. Create stored procedures to create views for dashboard analytics, so the processing is done in snowflake instead of the reporting tools. This depends on the update fact stored procedures and it won't start unless it completes. These are done using snowflake sql.
8.  Create the DAG by using snowflake tasks.
9. Test the pipeline abilitity to process incrementally by adding new data.

