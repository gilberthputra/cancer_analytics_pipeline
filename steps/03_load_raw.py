import time
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType

INCIDENCE_AND_MORTALITY_SCHEMA = StructType([ \
    StructField('DATA_TYPE', StringType()), \
    StructField('CANCER_GROUP_SITE', StringType()), \
    StructField('YEAR', IntegerType()), \
    StructField('SEX', StringType()), \
    StructField('AGE_GROUP_YEARS', StringType()), \
    StructField('COUNT', IntegerType()) \
])

SURVIVAL_SCHEMA = StructType([
    StructField('SURVIVAL_TYPE', StringType()),
    StructField('CANCER_GROUP_SITE', StringType()),
    StructField('PERIOD_YEARS', StringType()),
    StructField('SEX', StringType()),
    StructField('YEARS_AFTER_DIAGNOSIS', IntegerType()),
    StructField('AGE_GROUP_YEARS', StringType()),
    StructField('SURVIVAL', FloatType()),
    StructField('95_CI_LOWER_BOUND', FloatType()),
    StructField('95_CI_UPPER_BOUND', FloatType())
])

INCIDENCE_TERRITORY_SCHEMA = StructType([
    StructField('DATA_TYPE', StringType()), \
    StructField('CANCER_GROUP_SITE', StringType()), \
    StructField('YEAR', IntegerType()), \
    StructField('SEX', StringType()), \
    StructField('STATE_OR_TERRITORY', StringType()), \
    StructField('COUNT', IntegerType()) \
])

DATA_DICT = {"schema": "RAW_DATA", 
             "tables": {'incidence': INCIDENCE_AND_MORTALITY_SCHEMA,
                        'mortality': INCIDENCE_AND_MORTALITY_SCHEMA,
                        'survival': SURVIVAL_SCHEMA,
                        'incidence_territory': INCIDENCE_TERRITORY_SCHEMA}
            }

def load_raw_table(session, tname=None, tschema=None, dbschema=None):
    session.use_schema(dbschema)

    location = "@external.raw_data_stage/{}.csv.gz".format(tname)
    
    df = session.read \
            .schema(tschema) \
            .options({"field_delimiter": "|", "record_delimiter": "\n", "skip_header": 1}) \
            .csv(location)
    df.copy_into_table("{}".format(tname))

def load_all_raw_tables(session):
    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    dbschema = DATA_DICT['schema']
    table_dict = DATA_DICT['tables']

    for tname, tschema in table_dict.items():
        print("Loading {}".format(tname))
        load_raw_table(session, tname=tname, tschema=tschema, dbschema=dbschema)

    _ = session.sql("ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def validate_raw_tables(session):
    # check column names from the inferred schema
    for tname in DATA_DICT['tables'].keys():
        print('{}: \n\t{}\n'.format(tname, session.table('RAW_DATA.{}'.format(tname)).columns))

def create_table_streams(session):
    _ = session.sql("CREATE OR REPLACE STREAM CANCERANALYTICS_DB.RAW_DATA.RAW_INCIDENCE_STREAM \
                    ON TABLE CANCERANALYTICS_DB.RAW_DATA.INCIDENCE").collect()
    _ = session.sql("CREATE OR REPLACE STREAM CANCERANALYTICS_DB.RAW_DATA.RAW_MORTALITY_STREAM \
                    ON TABLE CANCERANALYTICS_DB.RAW_DATA.MORTALITY").collect()
    _ = session.sql("CREATE OR REPLACE STREAM CANCERANALYTICS_DB.RAW_DATA.RAW_SURVIVAL_STREAM \
                    ON TABLE CANCERANALYTICS_DB.RAW_DATA.SURVIVAL").collect()
    _ = session.sql("CREATE OR REPLACE STREAM CANCERANALYTICS_DB.RAW_DATA.RAW_TERRITORY_STREAM \
                    ON TABLE CANCERANALYTICS_DB.RAW_DATA.INCIDENCE_TERRITORY").collect()

if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_table_streams(session)
    load_all_raw_tables(session)
    validate_raw_tables(session)

    session.close()