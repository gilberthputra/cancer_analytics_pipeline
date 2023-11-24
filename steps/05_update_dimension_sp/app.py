#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       06_orders_process_sp/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Python Stored Procedures

import time
from snowflake.snowpark import Session, Window
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

DIM_TABLE = ['DATA_TYPE', 'CANCER_GROUP', 'SEX', 'AGE_GROUP', 'STATE_TERRITORY']

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_table(session, name=''):
    session.use_schema("TRANSFORMED_DATA")
    _ = session.sql(f"CREATE TABLE TRANSFORMED_DATA.{name} LIKE TEMP.{name}_VIEW").collect()
    _ = session.sql(f"ALTER TABLE TRANSFORMED_DATA.{name} ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def merge_updates(session, name=''):
    _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    source = session.table(f'TEMP.{name}_VIEW')
    target = session.table(f"TRANSFORMED_DATA.{name}")

    source.show()

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
        session.table(f'TRANSFORMED_DATA.{table_name}').limit(5).show()

    return f"Successfully update dimensions"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
