CREATE DATABASE IF NOT EXISTS CancerAnalyticsDB;
USE DATABASE CancerAnalyticsDB;
CREATE SCHEMA IF NOT EXISTS CancerAnalyticsDB.RawData;

CREATE TABLE CancerAnalyticsDB.RawData.RawIncidence(
    DataType VARCHAR(255),
    CancerGroupSite VARCHAR(255),
    Year INT,
    Sex VARCHAR(50),
    AgeGroup VARCHAR(50),
    Count INT,
    AgeSpecificRate FLOAT,
    AgeStandardisedRate2001 FLOAT,
    AgeStandardisedRate2023 FLOAT,
    AgeStandardisedRateWHO FLOAT,
    AgeStandardisedRateSegi FLOAT,
    ICD10Codes VARCHAR(255)
);

CREATE TABLE CancerAnalyticsDB.RawData.RawMortality (
    DataType VARCHAR(255),
    CancerGroupSite VARCHAR(255),
    Year INT,
    Sex VARCHAR(50),
    AgeGroup VARCHAR(50),
    Count INT,
    AgeSpecificRate FLOAT,
    AgeStandardisedRate2001 FLOAT,
    AgeStandardisedRate2023 FLOAT,
    AgeStandardisedRateWHO FLOAT,
    AgeStandardisedRateSegi FLOAT,
    ICD10Codes VARCHAR(255)
);

CREATE TABLE CancerAnalyticsDB.RawData.RawSurvival (
    SurvivalType VARCHAR(255),
    CancerGroupSite VARCHAR(255),
    Period VARCHAR(50),
    Sex VARCHAR(50),
    YearsAfterDiagnosis INT,
    AgeGroup VARCHAR(50),
    SurvivalRate VARCHAR(50),
    CILowerBound VARCHAR(50),
    CIUpperBound VARCHAR(50),
    ICD10Codes VARCHAR(255)
);

CREATE TABLE CancerAnalyticsDB.RawData.RawIncidenceByTerritory (
    DataType VARCHAR(255),
    CancerGroupSite VARCHAR(255),
    Year INT,
    Sex VARCHAR(50),
    StateOrTerritory VARCHAR(100),
    Count FLOAT,
    AgeStandardisedRate2001 VARCHAR(100),
    AgeStandardisedRate2023 VARCHAR(100),
    ICD10Codes VARCHAR(255)
);

CREATE STAGE CancerAnalyticsDB.RawData.RawDataStage
  file_format = (type = 'CSV' FIELD_DELIMITER = '|' RECORD_DELIMITER = '\n' SKIP_HEADER = 0);

PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/book1a_incidence.csv @CancerAnalyticsDB.RawData.RawDataStage;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/book2a_mortality.csv @CancerAnalyticsDB.RawData.RawDataStage;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/book3a_survival.csv @CancerAnalyticsDB.RawData.RawDataStage;
PUT file:///Users/gilbertharlimputra/Desktop/GitHub/cancer_dashboard_pipeline/data/transformed/book7_territory.csv @CancerAnalyticsDB.RawData.RawDataStage;