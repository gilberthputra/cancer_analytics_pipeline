CREATE OR REPLACE PROCEDURE CANCERANALYTICS_DB.RAW_DATA.UPDATE_FACT_SP()
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS CALLER
AS 
$$
import time
from snowflake.snowpark import Session, Window
import snowflake.snowpark.functions as F

DIM_TABLE = ['INCIDENCE', 'MORTALITY', 'SURVIVAL', 'INCIDENCE_TERRITORY']

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def view_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS VIEW_EXISTS".format(schema, name)).collect()[0]['VIEW_EXISTS']
    return exists

def create_table(session, name=''):
    session.use_schema("TRANSFORMED_DATA")
    _ = session.sql(f"CREATE TABLE TRANSFORMED_DATA.{name} LIKE TEMP.{name}_VIEW").collect()
    _ = session.sql(f"ALTER TABLE TRANSFORMED_DATA.{name} ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def delete_temp_view(session, name=''):
    session.use_schema("TEMP")
    _ = session.sql(f"DROP VIEW CANCERANALYTICS_DB.TEMP.{name}_VIEW").collect()

def merge_updates(session, name=''):
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    source = session.table(f'TEMP.{name}_VIEW')
    target = session.table(f"TRANSFORMED_DATA.{name}")

    # TODO: Is the if clause supposed to be based on "META_UPDATED_AT"?
    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    # merge into DIM_CUSTOMER
    target.merge(source, target[f'{name}_ID'] == source[f'{name}_ID'], \
                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    # Create the ORDERS table and ORDERS_STREAM stream if they don't exist
    
    for table_name in DIM_TABLE:
        if not table_exists(session, schema='TRANSFORMED_DATA', name=table_name):
            create_table(session, table_name)

        merge_updates(session, table_name)

        if table_exists(session, schema='TEMP', name=table_name):
            delete_temp_view(session, name)

    return f"Successfully update dimensions"
$$
