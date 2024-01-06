USE DATABASE CancerAnalyticsDB;
CREATE SCHEMA IF NOT EXISTS CancerAnalyticsDB.TransformedData;

CREATE TABLE CancerAnalyticsDB.TransformedData.DemographicDim (
    DemographicKey INTEGER PRIMARY KEY,
    Sex VARCHAR(10),
    AgeGroup VARCHAR(25)
);

CREATE TABLE CancerAnalyticsDB.TransformedData.CancerTypeDim (
    CancerTypeKey INTEGER PRIMARY KEY,
    CancerGroupSite VARCHAR(255),
    ICD10Codes VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS CancerAnalyticsDB.TransformedData.CancerSurvivalFacts (
    SurvivalFactID INT PRIMARY KEY,
    CancerTypeKey INT,
    DemographicKey INT,
    SurvivalType VARCHAR(10),
    PeriodYears VARCHAR(10),
    YearsAfterDiagnosis INT,
    SurvivalPercent FLOAT,
    CILowerBound FLOAT,
    CIUpperBound FLOAT,
    FOREIGN KEY (CancerTypeKey) REFERENCES CancerAnalyticsDB.TransformedData.CancerTypeDim(CancerTypeKey),
    FOREIGN KEY (DemographicKey) REFERENCES CancerAnalyticsDB.TransformedData.DemographicDim(DemographicKey)
);

CREATE TABLE IF NOT EXISTS CancerAnalyticsDB.TransformedData.CancerDataFacts (
    CancerDataID INT PRIMARY KEY,
    CancerTypeKey INT,
    DemographicKey INT,
    Year INT,
    IncidenceCount INT,
    MortalityCount INT,
    FOREIGN KEY (CancerTypeKey) REFERENCES CancerAnalyticsDB.TransformedData.CancerTypeDim(CancerTypeKey),
    FOREIGN KEY (DemographicKey) REFERENCES CancerAnalyticsDB.TransformedData.DemographicDim(DemographicKey)
);

CREATE TABLE IF NOT EXISTS CancerAnalyticsDB.TransformedData.CancerTerritoryFacts (
    CancerTerritoryID INT PRIMARY KEY,
    CancerTypeKey INT,
    DemographicKey INT,
    StateOrTerritory VARCHAR(100),
    Year INT,
    IncidenceCount INT,
    FOREIGN KEY (CancerTypeKey) REFERENCES CancerAnalyticsDB.TransformedData.CancerTypeDim(CancerTypeKey),
    FOREIGN KEY (DemographicKey) REFERENCES CancerAnalyticsDB.TransformedData.DemographicDim(DemographicKey),
);

