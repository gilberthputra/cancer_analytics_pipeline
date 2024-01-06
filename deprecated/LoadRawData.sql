USE DATABASE CANCERANALYTICSDB;

COPY INTO CANCERANALYTICSDB.RAWDATA.RawIncidence
FROM @CANCERANALYTICSDB.RAWDATA.RAWDATASTAGE/book1a_incidence.csv.gz
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 0);

COPY INTO CANCERANALYTICSDB.RAWDATA.RawMortality
FROM @CANCERANALYTICSDB.RAWDATA.RAWDATASTAGE/book2a_mortality.csv.gz
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 0);

COPY INTO CANCERANALYTICSDB.RAWDATA.RawSurvival
FROM @CANCERANALYTICSDB.RAWDATA.RAWDATASTAGE/book3a_survival.csv.gz
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 0);

COPY INTO CANCERANALYTICSDB.RAWDATA.RawIncidenceByTerritory
FROM @CANCERANALYTICSDB.RAWDATA.RAWDATASTAGE/book7_territory.csv.gz
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 0);