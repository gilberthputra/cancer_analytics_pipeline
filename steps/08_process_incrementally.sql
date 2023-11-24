USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE CANCERANALYTICS_DB;

-- ----------------------------------------------------------------------------
-- Step #1: Add new/remaining order data
-- ----------------------------------------------------------------------------

USE SCHEMA RAW_DATA;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE;

COPY INTO INCIDENCE
FROM @external.raw_data_stage/incidence_new.csv.gz
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.CSV_FORMAT);

COPY INTO MORTALITY
FROM @external.raw_data_stage/mortality_new.csv.gz
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.CSV_FORMAT);

COPY INTO INCIDENCE_TERRITORY
FROM @external.raw_data_stage/incidence_territory_new.csv.gz
FILE_FORMAT = (FORMAT_NAME = EXTERNAL.CSV_FORMAT);

-- See how many new records are in the stream (this may be a bit slow)
--SELECT COUNT(*) FROM HARMONIZED.POS_FLATTENED_V_STREAM;

ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL;

USE SCHEMA TRANSFORMED_DATA;
EXECUTE TASK TRANSFORM_DATA_TASK;