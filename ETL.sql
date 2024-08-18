CREATE DATABASE stage_area_1; -- Create a database for the staging area.
CREATE DATABASE dwh_1; -- Create a database for the data warehouse to store processed and structured data.

-- Create the `admissions_dim` table in the `stage_area_1` database. 
-- This table will store key details about each patient admission, including the patient ID, 
-- hospital admission ID, admission and discharge times, and locations within the hospital.

CREATE TABLE stage_area_1.admissions_dim AS 
SELECT 
    subject_id,
    hadm_id,
    admittime,
    dischtime,
    deathtime,
    admission_location,
    discharge_location
FROM mimic4.admissions;

alter table stage_area_1.admissions_dim 
ADD PRIMARY KEY (hadm_id);


-- Create the `patients` table in the `stage_area_1` database.
-- This table consolidates patient demographic information with admission details by joining the `patients` and `admissions` tables from the MIMIC-IV dataset.
CREATE TABLE stage_area_1.patients_dimension AS
SELECT
    p.subject_id,
    p.gender,
    p.anchor_age,
    p.anchor_year,
    p.anchor_year_group,
    a.ethnicity,
    a.insurance,
    a.language,
    a.marital_status
FROM
    mimic4.patients p
JOIN
    (
        SELECT DISTINCT
            a.subject_id,
            FIRST_VALUE(a.ethnicity) OVER (PARTITION BY a.subject_id ORDER BY a.admittime) as ethnicity,
            FIRST_VALUE(a.insurance) OVER (PARTITION BY a.subject_id ORDER BY a.admittime) as insurance,
            FIRST_VALUE(a.language) OVER (PARTITION BY a.subject_id ORDER BY a.admittime) as language,
            FIRST_VALUE(a.marital_status) OVER (PARTITION BY a.subject_id ORDER BY a.admittime) as marital_status
        FROM mimic4.admissions a
    ) a ON p.subject_id = a.subject_id;

alter table stage_area_1.patients_dimension 
ADD PRIMARY KEY (subject_id);

   -- Create the `date_dimension` table in the `stage_area_1` database.
-- This table serves as a time dimension in the data warehouse, storing distinct dates along with additional date attributes.
CREATE TABLE stage_area_1.date_dimension (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATETIME, -- Original datetime from various events
    corrected_date DATETIME, -- Adjusted date for handling specific edge cases (e.g., leap year)
    taarich_amiti DATETIME, -- Adjusted real-world date
    shana INT, -- Year
    chodesh INT, -- Month
    yom INT, -- Day
    shaa INT, -- Hour
    daka INT, -- Minute
    yom_shavua INT, -- Day of the week
    quarter INT, -- Quarter of the year
    semester INT -- Semester (first half or second half of the year)
);

-- Insert unique dates into the `date_dimension` table while converting them to real-world dates.
-- This process involves normalizing dates by adjusting leap year anomalies and converting them to a more meaningful date format.
INSERT INTO stage_area_1.date_dimension (full_date, corrected_date, taarich_amiti)
SELECT DISTINCT 
    full_date,
    CASE 
        WHEN MONTH(full_date) = 2 AND DAY(full_date) = 29 
        THEN DATE_SUB(full_date, INTERVAL 1 DAY) 
        ELSE full_date 
    END AS corrected_date,
    STR_TO_DATE(CONCAT(
        FLOOR((YEAR(CASE 
            WHEN MONTH(full_date) = 2 AND DAY(full_date) = 29 
            THEN DATE_SUB(full_date, INTERVAL 1 DAY) 
            ELSE full_date 
        END) / 25) + 1927), 
        DATE_FORMAT(CASE 
            WHEN MONTH(full_date) = 2 AND DAY(full_date) = 29 
            THEN DATE_SUB(full_date, INTERVAL 1 DAY) 
            ELSE full_date 
        END, '-%m-%d %H:%i:%s')
    ), '%Y-%m-%d %H:%i:%s') AS taarich_amiti
FROM (
    SELECT admittime AS full_date FROM mimic4.admissions
    UNION
    SELECT dischtime FROM  mimic4.admissions
    UNION
    SELECT deathtime FROM  mimic4.admissions
    UNION
    SELECT edregtime FROM  mimic4.admissions
    UNION
    SELECT edouttime FROM  mimic4.admissions
    UNION
    SELECT charttime FROM  mimic4.chartevents
    UNION
    SELECT storetime FROM   mimic4.chartevents
    UNION
    SELECT charttime FROM  mimic4.datetimeevents
    UNION
    SELECT storetime FROM  mimic4.datetimeevents
    UNION
    SELECT value FROM  mimic4.datetimeevents
    UNION
    SELECT charttime FROM  mimic4.emar
    UNION
    SELECT scheduletime FROM  mimic4.emar
    UNION
    SELECT storetime FROM  mimic4.emar
    UNION
    SELECT chartdate FROM  mimic4.hcpcsevents
    UNION
    SELECT intime FROM  mimic4.icustays
    UNION
    SELECT outtime FROM  mimic4.icustays
    UNION
    SELECT starttime FROM  mimic4.inputevents
    UNION
    SELECT endtime FROM  mimic4.inputevents
    UNION
    SELECT storetime FROM  mimic4.inputevents
    UNION
    SELECT charttime FROM  mimic4.labevents
    UNION
    SELECT storetime FROM  mimic4.labevents
    UNION
    SELECT chartdate FROM  mimic4.microbiologyevents
    UNION
    SELECT charttime FROM  mimic4.microbiologyevents
    UNION
    SELECT storedate FROM  mimic4.microbiologyevents
    UNION
    SELECT storetime FROM  mimic4.microbiologyevents
    UNION
    SELECT chartdate FROM  mimic4.omr
    UNION
    SELECT charttime FROM  mimic4.outputevents
    UNION
    SELECT storetime FROM  mimic4.outputevents
    UNION
    SELECT starttime FROM  mimic4.pharmacy
    UNION
    SELECT stoptime FROM  mimic4.pharmacy
    UNION
    SELECT entertime FROM  mimic4.pharmacy
    UNION
    SELECT verifiedtime FROM  mimic4.pharmacy
    UNION
    SELECT expirationdate FROM  mimic4.pharmacy
    UNION
    SELECT ordertime FROM  mimic4.poe
    UNION
    SELECT starttime FROM  mimic4.prescriptions
    UNION
    SELECT stoptime FROM  mimic4.prescriptions
    UNION
    SELECT starttime FROM  mimic4.procedureevents
    UNION
    SELECT endtime FROM  mimic4.procedureevents
    UNION
    SELECT storetime FROM  mimic4.procedureevents
    UNION
    SELECT chartdate FROM  mimic4.procedures_icd
    UNION
    SELECT transfertime FROM  mimic4.services
    UNION
    SELECT intime FROM  mimic4.transfers
    UNION
    SELECT outtime FROM  mimic4.transfers
) AS all_dates
WHERE full_date IS NOT NULL;

-- Update the `date_dimension` table with additional date attributes such as year, month, day, etc., based on the corrected real-world date.
UPDATE stage_area_1.date_dimension
SET shana = YEAR(taarich_amiti),
    chodesh = MONTH(taarich_amiti),
    yom = DAY(taarich_amiti),
    shaa = HOUR(taarich_amiti),
    daka = MINUTE(taarich_amiti),
    yom_shavua = DAYOFWEEK(taarich_amiti),
    quarter = QUARTER(taarich_amiti),
    semester = CASE
        WHEN MONTH(taarich_amiti) BETWEEN 1 AND 6 THEN 1
        ELSE 2
    END;

   
-- Create views to represent different time dimensions from the date_dimension table.
-- These views help in organizing and accessing specific date-related attributes for various time events such as admission, discharge, and death.

-- Create a view for admission times, based on the date_dimension table.
CREATE VIEW stage_area_1.admittime_v AS
SELECT * FROM stage_area_1.date_dimension;

-- Create a view for discharge times, based on the date_dimension table.
CREATE VIEW stage_area_1.dischtime_v AS
SELECT * FROM stage_area_1.date_dimension;

-- Create a view for death times, based on the date_dimension table.
CREATE VIEW stage_area_1.deathtime_v AS
SELECT * FROM stage_area_1.date_dimension;


-- Create a table for storing diagnosis-related information with unique diagnosis codes.
CREATE TABLE stage_area_1.diagnoses_dimension (
    diagnosis_cd VARCHAR(10) NOT NULL,  -- A unique identifier for each diagnosis, combining ICD code and version.
    icd_code VARCHAR(10),               
    icd_version INTEGER ,                
    diagnosis_name VARCHAR(1000),       
    PRIMARY KEY (diagnosis_cd)         
);

-- Insert unique diagnosis data into the diagnoses_dimension table.
INSERT INTO stage_area_1.diagnoses_dimension (diagnosis_cd, icd_code, icd_version, diagnosis_name)
SELECT DISTINCT
    CONCAT(CAST(d.icd_code AS VARCHAR(20)), '_', CAST(d.icd_version AS VARCHAR(20))) AS diagnosis_cd,  -- Create a unique diagnosis code by combining ICD code and version.
    d.icd_code,
    d.icd_version,
    d.long_title AS diagnosis_name
FROM mimic4.d_icd_diagnoses d;  -- Select data from the source table in MIMIC-IV to populate the diagnoses dimension.


-- Create a table for storing provider-related information, including event types and care units.
CREATE TABLE stage_area_1.provider_dimension (
    provider_cd INTEGER NOT NULL AUTO_INCREMENT,  -- A unique identifier for each provider, automatically incremented.
    eventtype VARCHAR(50),                       
    careunit VARCHAR(1000),                      
    PRIMARY KEY (provider_cd)                    
);

-- Insert distinct provider data into the provider_dimension table from the transfers table in MIMIC-IV.
INSERT INTO stage_area_1.provider_dimension
SELECT DISTINCT 
    null AS provider_cd,                         
    eventtype,                                   
    CASE WHEN (careunit IS NULL) THEN ('discharge') ELSE (careunit) END AS careunit  -- Replace null care units with 'discharge'.
FROM mimic4.transfers t 
WHERE eventtype IS NOT NULL AND eventtype = 'admit'; 

-- Create a view that integrates corrected, real-world dates into the admissions data using the date dimension.
CREATE VIEW stage_area_1.admissions_v AS
SELECT 
    a.subject_id,                       
    a.hadm_id,                          
    admittime_v.taarich_amiti AS admittime, 
    dischtime_v.taarich_amiti AS dischtime, 
    deathtime_v.taarich_amiti AS deathtime, 
    a.admission_type,                  
    a.admit_provider_id,               
    a.admission_location,               
    a.discharge_location,               
    a.insurance,                        
    a.language,                         
    a.marital_status,                   
    a.ethnicity                         
FROM mimic4.admissions a 
LEFT OUTER JOIN stage_area_1.admittime_v AS admittime_v
    ON a.admittime = admittime_v.full_date
LEFT OUTER JOIN stage_area_1.dischtime_v AS dischtime_v 
    ON a.dischtime = dischtime_v.full_date
LEFT OUTER JOIN stage_area_1.deathtime_v AS deathtime_v
    ON a.deathtime = deathtime_v.full_date;
   
   
-- Create a dimension table for blood product items by filtering items related to blood in input events.
CREATE TABLE stage_area_1.blood_product_dim AS
SELECT * 
FROM mimic4.d_items
WHERE itemid IN (
    SELECT DISTINCT itemid 
    FROM mimic4.inputevents 
    WHERE LOWER(ordercategoryname) LIKE '%blood%'
);

alter table stage_area_1.blood_product_dim 
ADD PRIMARY KEY (itemid);

-- Create a table to store concept-related metadata with a unique concept path for each entry.
CREATE TABLE stage_area_1.concept_dimension (
  `CONCEPT_PATH` varchar(700) NOT NULL,
  `CONCEPT_CD` varchar(50) NOT NULL,
  `NAME_CHAR` varchar(2000) DEFAULT NULL,
  `UPDATE_DATE` timestamp NULL DEFAULT NULL,
  `DOWNLOAD_DATE` timestamp NULL DEFAULT NULL,
  `IMPORT_DATE` timestamp NULL DEFAULT NULL,
  `SOURCESYSTEM_CD` varchar(50) DEFAULT NULL,
  `UPLOAD_ID` int(11) DEFAULT NULL,
  PRIMARY KEY (`CONCEPT_CD`)
);

-- Insert data into the concept_dimension table from d_items, focusing on blood product items.
INSERT INTO stage_area_1.concept_dimension (
  CONCEPT_PATH,
  CONCEPT_CD,
  NAME_CHAR,
  UPDATE_DATE,
  DOWNLOAD_DATE,
  IMPORT_DATE,
  SOURCESYSTEM_CD,
  UPLOAD_ID
)
SELECT 
  CONCAT('/mimic4/items/', REPLACE(itemid, ' ', '_')) AS CONCEPT_PATH,
  CAST(itemid AS CHAR) AS CONCEPT_CD,
  label AS NAME_CHAR,
  CURRENT_TIMESTAMP AS UPDATE_DATE,
  NULL AS DOWNLOAD_DATE,
  NULL AS IMPORT_DATE,
  'mimic4' AS SOURCESYSTEM_CD,
  1 AS UPLOAD_ID
FROM 
  mimic4.d_items di 
WHERE 
  itemid IN (SELECT itemid FROM stage_area_1.blood_product_dim);

-- Insert data from the diagnoses_dimension table into the concept_dimension table.
INSERT INTO stage_area_1.concept_dimension (
  CONCEPT_PATH,
  CONCEPT_CD,
  NAME_CHAR,
  UPDATE_DATE,
  DOWNLOAD_DATE,
  IMPORT_DATE,
  SOURCESYSTEM_CD,
  UPLOAD_ID
)
SELECT 
  CONCAT('/stage_area_1/diagnoses_dimension/', REPLACE(diagnosis_cd, ' ', '_')) AS CONCEPT_PATH,
  diagnosis_cd AS CONCEPT_CD,
  diagnosis_name AS NAME_CHAR,
  CURRENT_TIMESTAMP AS UPDATE_DATE,
  NULL AS DOWNLOAD_DATE,
  NULL AS IMPORT_DATE,
  'stage_area_1' as SOURCESYSTEM_CD,
  1 UPLOAD_ID
FROM 
  stage_area_1.diagnoses_dimension;

-- Insert distinct result names from the omr table into the concept_dimension table.
INSERT INTO stage_area_1.concept_dimension (
  CONCEPT_PATH,
  CONCEPT_CD,
  NAME_CHAR,
  UPDATE_DATE,
  DOWNLOAD_DATE,
  IMPORT_DATE,
  SOURCESYSTEM_CD,
  UPLOAD_ID
)
SELECT 
  CONCAT('/mimic4/omr/', REPLACE(result_name, ' ', '_')) AS CONCEPT_PATH,
  result_name AS CONCEPT_CD,
  result_name AS NAME_CHAR,
  CURRENT_TIMESTAMP AS UPDATE_DATE,
  NULL AS DOWNLOAD_DATE,
  NULL AS IMPORT_DATE,
  'mimic4' as SOURCESYSTEM_CD,
  1 UPLOAD_ID
FROM 
  (SELECT DISTINCT(result_name) FROM mimic4.omr) AS omm;
 
-- Create a table to store observational data related to patient encounters, including details about the concept, provider, and associated measurements or values.
CREATE TABLE stage_area_1.observation_fact (
  `ENCOUNTER_NUM` int(10) unsigned NOT NULL,
  `PATIENT_NUM` int(10) unsigned NOT NULL, 
  `CONCEPT_CD` varchar(50) NOT NULL, 
  `PROVIDER_ID` int (11) NOT NULL, 
  `START_DATE` int(11) NOT NULL,
  `END_DATE` int(10) NULL DEFAULT NULL, 
  `VALTYPE_CD` varchar(50) DEFAULT NULL,
  `TVAL_CHAR` varchar(255) DEFAULT NULL, 
  `VALUEFLAG_CD` varchar(50) DEFAULT NULL, 
  `QUANTITY_NUM` decimal(18,5) DEFAULT NULL, 
  `CONFIDENCE_NUM` decimal(18,5) DEFAULT NULL, 
  `RATE-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `AMOUNT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `ORIGINALAMOUNT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `ORIGINALRATE-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `PATIENTWEIGHT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `UPDATE_DATE` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(), 
  `DOWNLOAD_DATE` timestamp NULL DEFAULT NULL, 
  `IMPORT_DATE` timestamp NULL DEFAULT NULL, 
  `SOURCESYSTEM_CD` varchar(50) DEFAULT NULL, 
  `UPLOAD_ID` int(11) DEFAULT null,
  FOREIGN KEY (`ENCOUNTER_NUM`) REFERENCES stage_area_1.admissions_dim(`hadm_id`), 
  FOREIGN KEY (`PATIENT_NUM`) REFERENCES stage_area_1.patients_dimension(`subject_id`),
  FOREIGN KEY (`CONCEPT_CD`) REFERENCES stage_area_1.concept_dimension(`CONCEPT_CD`),
  FOREIGN KEY (`PROVIDER_ID`) REFERENCES stage_area_1.provider_dimension(`provider_cd`),
  FOREIGN KEY (`START_DATE`) REFERENCES stage_area_1.date_dimension(`date_id`),
  FOREIGN KEY (`END_DATE`) REFERENCES stage_area_1.date_dimension(`date_id`) 
) 


-- This query inserts data into the observation_fact table from a combination of omr, admissions, and transfers tables,
-- with additional mappings from provider_dimension and date_dimension tables.

INSERT INTO stage_area_1.observation_fact (
  `ENCOUNTER_NUM`,
  `PATIENT_NUM`,
  `CONCEPT_CD`,
  `PROVIDER_ID`,
  `START_DATE`,
  `END_DATE`,
  `VALTYPE_CD`,
  `TVAL_CHAR`,
  `VALUEFLAG_CD`,
  `QUANTITY_NUM`,
  `CONFIDENCE_NUM`,
  `RATE-NVAL_NUM`,
  `AMOUNT-NVAL_NUM`,
  `ORIGINALAMOUNT-NVAL_NUM`,
  `ORIGINALRATE-NVAL_NUM`,
  `PATIENTWEIGHT-NVAL_NUM`,
  `SOURCESYSTEM_CD`,
  `UPLOAD_ID`
)
SELECT 
  tt.hadm_id AS ENCOUNTER_NUM,
  tt.subject_id AS PATIENT_NUM,
  tt.result_name AS CONCEPT_CD,
  pd.provider_cd AS PROVIDER_ID,
  dd.date_id  AS START_DATE,
  NULL AS END_DATE,
  'T' AS VALTYPE_CD,
  tt.result_value AS TVAL_CHAR,
  NULL AS VALUEFLAG_CD,
  NULL AS QUANTITY_NUM,
  NULL AS CONFIDENCE_NUM,
  NULL AS `RATE-NVAL_NUM`,
  NULL AS `AMOUNT-NVAL_NUM`,
  NULL AS `ORIGINALAMOUNT-NVAL_NUM`,
  NULL AS `ORIGINALRATE-NVAL_NUM`,
  NULL AS `PATIENTWEIGHT-NVAL_NUM`,
  NULL AS SOURCESYSTEM_CD,
  NULL AS UPLOAD_ID
FROM (
    SELECT o2.subject_id, t.hadm_id, o2.chartdate, o2.result_name, o2.result_value, t.careunit, t.eventtype
    FROM mimic4.omr o2
    INNER JOIN (
        SELECT o.subject_id, a.hadm_id, MAX(o.chartdate) AS chartdate, t.careunit, t.eventtype
        FROM mimic4.omr o
        INNER JOIN mimic4.admissions a 
        ON o.subject_id = a.subject_id 
        INNER JOIN mimic4.transfers t 
        ON o.subject_id = t.subject_id AND t.eventtype = 'admit'
        WHERE o.chartdate BETWEEN ADDDATE(a.admittime, INTERVAL -2 YEAR) AND a.dischtime 
        GROUP BY o.subject_id, a.hadm_id, t.careunit, t.eventtype
    ) t
    ON o2.subject_id = t.subject_id AND o2.chartdate = t.chartdate
) AS tt 
LEFT OUTER JOIN stage_area_1.provider_dimension pd 
ON pd.eventtype = tt.eventtype AND pd.careunit = tt.careunit
LEFT OUTER JOIN stage_area_1.date_dimension dd
ON tt.chartdate = dd.full_date
WHERE 
  tt.result_name IS NOT NULL;

-- This query inserts data into the observation_fact table from the diagnoses_icd table,
-- combining it with relevant information from admissions_dim, diagnoses_dimension,
-- date_dimension, transfers, and provider_dimension tables.

INSERT INTO stage_area_1.observation_fact (
  `ENCOUNTER_NUM`,
  `PATIENT_NUM`,
  `CONCEPT_CD`,
  `PROVIDER_ID`,
  `START_DATE`,
  `END_DATE`,
  `VALTYPE_CD`,
  `TVAL_CHAR`,
  `VALUEFLAG_CD`,
  `QUANTITY_NUM`,
  `CONFIDENCE_NUM`,
  `RATE-NVAL_NUM`,
  `AMOUNT-NVAL_NUM`,
  `ORIGINALAMOUNT-NVAL_NUM`,
  `ORIGINALRATE-NVAL_NUM`,
  `PATIENTWEIGHT-NVAL_NUM`,
  `SOURCESYSTEM_CD`,
  `UPLOAD_ID`
)
SELECT 
  d.hadm_id AS ENCOUNTER_NUM, 
  d.subject_id AS PATIENT_NUM, 
  dd.diagnosis_cd AS CONCEPT_CD,
  pd.provider_cd AS PROVIDER_ID,
  start_date_dim.date_id AS START_DATE,
  end_date_dim.date_id AS END_DATE,
  'N' AS VALTYPE_CD,
  NULL AS TVAL_CHAR,
  NULL AS VALUEFLAG_CD,
  NULL AS QUANTITY_NUM,
  NULL AS CONFIDENCE_NUM,
  NULL AS `RATE-NVAL_NUM`,
  NULL AS `AMOUNT-NVAL_NUM`,
  NULL AS `ORIGINALAMOUNT-NVAL_NUM`,
  NULL AS `ORIGINALRATE-NVAL_NUM`,
  NULL AS `PATIENTWEIGHT-NVAL_NUM`,
  NULL AS SOURCESYSTEM_CD,
  NULL AS UPLOAD_ID
FROM mimic4.diagnoses_icd d
LEFT OUTER JOIN stage_area_1.admissions_dim ad 
    ON d.subject_id = ad.subject_id AND d.hadm_id = ad.hadm_id 
LEFT OUTER JOIN stage_area_1.date_dimension start_date_dim 
    ON ad.admittime = start_date_dim.full_date  
LEFT OUTER JOIN stage_area_1.date_dimension end_date_dim 
    ON ad.dischtime = end_date_dim.full_date    
LEFT OUTER JOIN stage_area_1.diagnoses_dimension dd 
    ON d.icd_code = dd.icd_code AND d.icd_version = dd.icd_version
LEFT OUTER JOIN mimic4.transfers t 
    ON d.subject_id = t.subject_id AND d.hadm_id = t.hadm_id AND t.eventtype = 'admit'
LEFT OUTER JOIN stage_area_1.provider_dimension pd 
    ON t.eventtype = pd.eventtype AND t.careunit = pd.careunit
WHERE d.icd_code IN (
    SELECT DISTINCT di.icd_code
    FROM mimic4.diagnoses_icd di 
    JOIN (
        SELECT * 
        FROM mimic4.inputevents i 
        WHERE itemid IN (
            SELECT itemid 
            FROM stage_area_1.blood_product_dim
        )
    ) AS bl
    WHERE di.hadm_id = bl.hadm_id
);

-- This query inserts data into the observation_fact table by joining inputevents and blood_product_dim data with related tables to enrich the dataset with provider information, dates, and other attributes. 

INSERT INTO stage_area_1.observation_fact (
  `ENCOUNTER_NUM`,
  `PATIENT_NUM`,
  `CONCEPT_CD`,
  `PROVIDER_ID`,
  `START_DATE`,
  `END_DATE`,
  `VALTYPE_CD`,
  `TVAL_CHAR`,
  `VALUEFLAG_CD`,
  `QUANTITY_NUM`,
  `CONFIDENCE_NUM`,
  `RATE-NVAL_NUM`,
  `AMOUNT-NVAL_NUM`,
  `ORIGINALAMOUNT-NVAL_NUM`,
  `ORIGINALRATE-NVAL_NUM`,
  `PATIENTWEIGHT-NVAL_NUM`,
  `SOURCESYSTEM_CD`,
  `UPLOAD_ID`
)
SELECT 
  bld.hadm_id AS `ENCOUNTER_NUM`,
  bld.subject_id AS `PATIENT_NUM`,
  bld.itemid AS `CONCEPT_CD`,
  pd.provider_cd AS `PROVIDER_ID`,
  dd.date_id AS `START_DATE`,
  ddd.date_id AS `END_DATE`,
  'N' AS `VALTYPE_CD`,  -- Assuming numeric values for the inserted data
  ordercategoryname AS `TVAL_CHAR`,  -- No text value to insert
  bld.isopenbag AS `VALUEFLAG_CD`,  -- Mapping isopenbag to VALUEFLAG_CD
  ROUND(bld.totalamount, 2) AS `QUANTITY_NUM`,  -- Total amount as quantity
  NULL AS `CONFIDENCE_NUM`,  -- Assuming no confidence score
  ROUND(bld.rate, 2) AS `RATE-NVAL_NUM`,
  ROUND(bld.amount, 2) AS `AMOUNT-NVAL_NUM`,
  ROUND(bld.originalamount, 2) AS `ORIGINALAMOUNT-NVAL_NUM`,
  ROUND(bld.originalrate, 2) AS `ORIGINALRATE-NVAL_NUM`,
  ROUND(bld.patientweight, 2) AS `PATIENTWEIGHT-NVAL_NUM`,
  NULL AS `SOURCESYSTEM_CD`,  -- Assuming no source system code
  NULL AS `UPLOAD_ID`  -- Assuming no upload ID
FROM 
  (
    SELECT * 
    FROM mimic4.inputevents i 
    WHERE itemid IN (SELECT itemid FROM stage_area_1.blood_product_dim)
  ) AS bld
JOIN 
  mimic4.transfers t 
ON 
  bld.subject_id = t.subject_id AND bld.hadm_id = t.hadm_id 
JOIN 
  stage_area_1.provider_dimension pd 
ON 
  t.careunit = pd.careunit
JOIN 
  stage_area_1.date_dimension dd 
ON 
  bld.starttime = dd.full_date
JOIN 
  stage_area_1.date_dimension ddd
ON 
  bld.endtime = ddd.full_date
WHERE 
  t.eventtype = 'admit';
-- -----------------------------------------------------------------------------------------------------------------
-- Load data from stage area to the data warehouse
 
-- Table for admissions dimension
CREATE TABLE dwh_1.admissions_dim (
    subject_id int(10) unsigned NOT NULL,
    hadm_id int(10) unsigned NOT NULL,
    admittime DATETIME,
    dischtime DATETIME,
    deathtime DATETIME,
    admission_location VARCHAR(255),
    discharge_location VARCHAR(255),
    PRIMARY KEY (hadm_id)
);

-- Insert data into admissions_dim
INSERT INTO dwh_1.admissions_dim
SELECT * FROM stage_area_1.admissions_dim;

-- Table for blood product dimension
CREATE TABLE dwh_1.blood_product_dim (
    itemid INT NOT NULL,
    label text,
    abbreviation VARCHAR(255),
    linksto VARCHAR(255),
    category VARCHAR(255),
    unitname VARCHAR(255),
    param_type VARCHAR(255),
    lownormalvalue smallint(6),
    highnormalvalue float,
    PRIMARY KEY (itemid)
);

-- Insert data into blood_product_dim
INSERT INTO dwh_1.blood_product_dim
SELECT * FROM stage_area_1.blood_product_dim;


-- Table for concept dimension
CREATE TABLE dwh_1.concept_dimension (
    CONCEPT_PATH VARCHAR(700) NOT NULL,
    CONCEPT_CD VARCHAR(50) NOT NULL,
    NAME_CHAR VARCHAR(2000),
    UPDATE_DATE TIMESTAMP NULL DEFAULT NULL,
    DOWNLOAD_DATE TIMESTAMP NULL DEFAULT NULL,
    IMPORT_DATE TIMESTAMP NULL DEFAULT NULL,
    SOURCESYSTEM_CD VARCHAR(50),
    UPLOAD_ID INT (11),
    PRIMARY KEY (CONCEPT_CD)
);

-- Insert data into concept_dimension
INSERT INTO dwh_1.concept_dimension
SELECT * FROM stage_area_1.concept_dimension;


-- Table for date dimension
CREATE TABLE dwh_1.date_dimension (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATETIME,
    corrected_date DATETIME,
    taarich_amiti DATETIME,
    shana INT,
    chodesh INT,
    yom INT,
    shaa INT,
    daka INT,
    yom_shavua INT,
    quarter INT,
    semester INT
);

-- Insert data into date_dimension
INSERT INTO dwh_1.date_dimension
SELECT * FROM stage_area_1.date_dimension;


CREATE TABLE dwh_1.diagnoses_dimension (
    diagnosis_cd VARCHAR(10) NOT NULL,
    icd_code VARCHAR(10),
    icd_version INT,
    diagnosis_name VARCHAR(1000),
    PRIMARY KEY (diagnosis_cd)
);

-- Insert data into diagnoses_dimension
INSERT INTO dwh_1.diagnoses_dimension
SELECT * FROM stage_area_1.diagnoses_dimension;



-- Table for patients dimension
CREATE TABLE dwh_1.patients_dimension (
    subject_id INT NOT NULL,
    gender VARCHAR(255),
    anchor_age tinyint(3) unsigned,
    anchor_year smallint(5) unsigned,
    anchor_year_group VARCHAR(255),
    ethnicity VARCHAR(255),
    insurance VARCHAR(255),
    language VARCHAR(255),
    marital_status VARCHAR(255),
    PRIMARY KEY (subject_id)
);

-- Insert data into patients_dimension
INSERT INTO dwh_1.patients_dimension
SELECT * FROM stage_area_1.patients_dimension;

-- Table for provider dimension
CREATE TABLE dwh_1.provider_dimension (
    provider_cd INT NOT NULL AUTO_INCREMENT,
    eventtype VARCHAR(50),
    careunit VARCHAR(1000),
    PRIMARY KEY (provider_cd)
);

-- Insert data into provider_dimension
INSERT INTO dwh_1.provider_dimension
SELECT * FROM stage_area_1.provider_dimension;


-- Create a table to store observational data related to patient encounters, including details about the concept, provider, and associated measurements or values.
CREATE TABLE dwh_1.observation_fact (
  `ENCOUNTER_NUM` int(10) unsigned NOT NULL,
  `PATIENT_NUM` int(11)  NOT NULL, 
  `CONCEPT_CD` varchar(50) NOT NULL, 
  `PROVIDER_ID` int (11) NOT NULL, 
  `START_DATE` int(11) NOT NULL,
  `END_DATE` int(11) NULL DEFAULT NULL, 
  `VALTYPE_CD` varchar(50) DEFAULT NULL,
  `TVAL_CHAR` varchar(255) DEFAULT NULL, 
  `VALUEFLAG_CD` varchar(50) DEFAULT NULL, 
  `QUANTITY_NUM` decimal(18,5) DEFAULT NULL, 
  `CONFIDENCE_NUM` decimal(18,5) DEFAULT NULL, 
  `RATE-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `AMOUNT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `ORIGINALAMOUNT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `ORIGINALRATE-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `PATIENTWEIGHT-NVAL_NUM` decimal(18,5) DEFAULT NULL, 
  `UPDATE_DATE` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(), 
  `DOWNLOAD_DATE` timestamp NULL DEFAULT NULL, 
  `IMPORT_DATE` timestamp NULL DEFAULT NULL, 
  `SOURCESYSTEM_CD` varchar(50) DEFAULT NULL, 
  `UPLOAD_ID` int(11) DEFAULT null,
  FOREIGN KEY (`ENCOUNTER_NUM`) REFERENCES dwh_1.admissions_dim(`hadm_id`), 
  FOREIGN KEY (`PATIENT_NUM`) REFERENCES dwh_1.patients_dimension(`subject_id`),
  FOREIGN KEY (`CONCEPT_CD`) REFERENCES dwh_1.concept_dimension(`CONCEPT_CD`),
  FOREIGN KEY (`PROVIDER_ID`) REFERENCES dwh_1.provider_dimension(`provider_cd`),
  FOREIGN KEY (`START_DATE`) REFERENCES dwh_1.date_dimension(`date_id`),
  FOREIGN KEY (`END_DATE`) REFERENCES dwh_1.date_dimension(`date_id`) 
) 

-- Insert data into provider_dimension
INSERT INTO dwh_1.observation_fact
SELECT * FROM stage_area_1.observation_fact;



-- ------------------------------------------------------------------------------------------------------------------
-- Aggregation query

SELECT 
    ENCOUNTER_NUM,
    COUNT(ENCOUNTER_NUM) as procedures_amount,
    SUM(QUANTITY_NUM) as total_product_to_admission_ml,
    sum(TIMESTAMPDIFF(minute, dd_start.taarich_amiti, dd_end.taarich_amiti)/60.0) AS total_procedures_time_hours
FROM 
    dwh_1.observation_fact of2
JOIN 
    dwh_1.date_dimension dd_start ON of2.START_DATE = dd_start.date_id
JOIN 
    dwh_1.date_dimension dd_end ON of2.END_DATE = dd_end.date_id
WHERE 
    CONCEPT_CD = (
        SELECT CONCEPT_CD 
        FROM stage_area_1.concept_dimension cd 
        WHERE NAME_CHAR = 'Packed Red Blood Cells'
    )
GROUP BY 
    ENCOUNTER_NUM
    order by total_product_to_admission_ml desc
   
  