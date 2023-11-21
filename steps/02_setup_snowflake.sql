-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE HOL_ROLE;
GRANT ROLE HOL_ROLE TO ROLE SYSADMIN;
GRANT ROLE HOL_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE HOL_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE HOL_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE HOL_ROLE;

-- Databases
CREATE OR REPLACE DATABASE CANCERANALYTICS_DB;
GRANT OWNERSHIP ON DATABASE CANCERANALYTICS_DB TO ROLE HOL_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE HOL_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE HOL_WH TO ROLE HOL_ROLE;

-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE CANCERANALYTICS_DB;

-- Schemas
CREATE OR REPLACE SCHEMA EXTERNAL;
CREATE OR REPLACE SCHEMA RAW_DATA;
CREATE OR REPLACE SCHEMA VIEWS;

-- External Frostbyte objects
USE SCHEMA EXTERNAL;
CREATE OR REPLACE FILE FORMAT CANCERANALYTICS_DB.EXTERNAL.CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = '|' 
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
;

CREATE OR REPLACE STAGE CANCERANALYTICS_DB.EXTERNAL.RAW_DATA_STAGE
    file_format = CSV_FORMAT
;

PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/incidence.csv @CANCERANALYTICS_DB.EXTERNAL.RAW_DATA_STAGE;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/mortality.csv @CANCERANALYTICS_DB.EXTERNAL.RAW_DATA_STAGE;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/survival.csv @CANCERANALYTICS_DB.EXTERNAL.RAW_DATA_STAGE;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/incidence_territory.csv @CANCERANALYTICS_DB.EXTERNAL.RAW_DATA_STAGE;