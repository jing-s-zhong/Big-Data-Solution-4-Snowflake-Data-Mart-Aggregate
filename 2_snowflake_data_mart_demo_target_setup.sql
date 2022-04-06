-- 
-- default database should be ASL or ANALTICS
--
USE DATABASE ASL;

/********************************************************************
 ** Data Mart Schema Create Section (Kimball Model)
 ********************************************************************/
--
-- Create DW tables
--
DROP SCHEMA IF EXISTS ANALYSIS;
CREATE OR REPLACE TRANSIENT SCHEMA ANALYSIS;

DROP TABLE IF EXISTS ANALYSIS.DIM_DATE;
CREATE OR REPLACE TRANSIENT TABLE ANALYSIS.DIM_DATE 
(
   DATA_DATE        DATE        NOT NULL
  ,YEAR             SMALLINT    NOT NULL
  ,MONTH            SMALLINT    NOT NULL
  ,MONTH_NAME       CHAR(3)     NOT NULL
  ,DAY_OF_MON       SMALLINT    NOT NULL
  ,DAY_OF_WEEK      VARCHAR(9)  NOT NULL
  ,WEEK_OF_YEAR     SMALLINT    NOT NULL
  ,DAY_OF_YEAR      SMALLINT    NOT NULL
)
AS
WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2022-01-01') AS DATA_DATE
    FROM TABLE(GENERATOR(ROWCOUNT=>365))  -- Number of days after reference date in previous line
)
SELECT DATA_DATE
    ,YEAR(DATA_DATE)
    ,MONTH(DATA_DATE)
    ,MONTHNAME(DATA_DATE)
    ,DAY(DATA_DATE)DATA_DATE
    ,DAYOFWEEK(DATA_DATE)
    ,WEEKOFYEAR(DATA_DATE)
    ,DAYOFYEAR(DATA_DATE)
FROM CTE_MY_DATE
;

DROP TABLE IF EXISTS ANALYSIS.DIM_PLATFORM;
CREATE OR REPLACE TRANSIENT TABLE ANALYSIS.DIM_PLATFORM (
    PLATFORM_ID NUMBER, 
    PLATFORM_NAME VARCHAR, 
    PLATFORM_TYPE VARCHAR
)
AS
SELECT PLATFORM_ID
    ,PLATFORM_NAME
    ,PLATFORM_TYPE
FROM INT.REFERENCE.PLATFORM
;

DROP TABLE IF EXISTS ANALYSIS.DIM_ORGANIZATION;
CREATE OR REPLACE TRANSIENT TABLE ANALYSIS.DIM_ORGANIZATION (
    ORGANIZATION_ID NUMBER, 
    ORGANIZATION_NAME VARCHAR, 
    SHORT_NAME VARCHAR
)
AS
SELECT ORGANIZATION_ID
    ,ORGANIZATION_NAME
    ,SHORT_NAME
FROM INT.REFERENCE.ORGANIZATION
;

DROP TABLE IF EXISTS ANALYSIS.DIM_PERSON;
CREATE OR REPLACE TABLE ANALYSIS.DIM_PERSON (
	PERSON_ID NUMBER IDENTITY, 
	FULL_NAME TEXT, 
	FIRST_NAME TEXT, 
	LAST_NAME TEXT, 
	TITLE TEXT, 
	PHOTO_URL TEXT, 
	VIEWABLE BOOLEAN DEFAULT TRUE,
	SCORE FLOAT, 
    PLATFORM_ID NUMBER,
    ORGANIZATION_ID NUMBER, 
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

DROP TABLE IF EXISTS ANALYSIS.FACT_CONTACT;
CREATE OR REPLACE TABLE ANALYSIS.FACT_CONTACT (
	PERSON_CONTACT_ID NUMBER IDENTITY,
	PERSON_ID NUMBER, 
	CONTACT_ID NUMBER, 
    INTERNAL_CONTACT BOOLEAN,
    VIEWABLE_CONTACT BOOLEAN DEFAULT TRUE,
    RELATIONSHIP_SCORE FLOAT,
    LAST_ACTIVITY_TIME TIMESTAMP_NTZ,
	BUSINESS_RUN_DATE DATE, 
    PLATFORM_ID NUMBER,
	DATA_KEY TEXT, 
	DATA_HASH TEXT, 
    LOAD_TIME TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT TIMESTAMP_NTZ,
    VALID_FROM DATE DEFAULT CURRENT_DATE,
    VALID_TO DATE
);

/********************************************************************
 ** Data Warehouse Load Target Configuration 
 ********************************************************************/
--
-- Create warehouse target config data
--
USE SCHEMA _METADATA;
/*
TRUNCATE TABLE CTRL_TARGET;
TRUNCATE TABLE CTRL_SOURCE;
*/

MERGE INTO CTRL_TARGET D
USING (
    SELECT $1 TARGET_LABEL,
        $2 TARGET_TYPE,
        $3 TARGET_DATA,
        $4 HISTORY_DATA,
        $5 PROCESS_PRIORITY,
        $6 SCD_TYPE,
        PARSE_JSON('{
            "DATA_KEY_FIELD": "DATA_KEY", 
            "DATA_HASH_FIELD": "DATA_HASH",
            "DATA_TIME_FIELD": "LOAD_TIME",
            "VALID_FROM_FIELD": "VALID_FROM",
            "VALID_TO_FIELD": "VALID_TO"
            }') CTRL_FIELD,
        PARSE_JSON($7) DATA_FIELD,
        PARSE_JSON($8) META_FIELD
    FROM VALUES 
    (
        'DIM_PERSON',
        'DIM',
        'ANALYSIS.DIM_PERSON',
        'HISTORY.DIM_PERSON',
        1,
        1,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FULL_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "FIRST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_NAME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "TITLE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PHOTO_URL",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "ORGANIZATION_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    ),
    (
        'FACT_CONTACT',
        'FACT',
        'ANALYSIS.FACT_CONTACT',
        'HISTORY.FACT_CONTACT',
        1,
        1,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_CONTACT_ID",
                "FIELD_TRANS": "IDENTITY",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "PERSON_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "CONTACT_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "INTERNAL_CONTACT",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "BOOLEAN"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "RELATIONSHIP_SCORE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "FLOAT"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LAST_ACTIVITY_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": false,
                "FIELD_NAME": "BUSINESS_RUN_DATE",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "DATE"
            },
            {
                "FIELD_FOR_HASH": true,
                "FIELD_FOR_KEY": true,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "PLATFORM_ID",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "NUMBER"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_KEY",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "DATA_HASH",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TEXT"
            },
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "LOAD_TIME",
                "FIELD_TRANS": "",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$,
        $$[
            {
                "FIELD_FOR_HASH": false,
                "FIELD_FOR_KEY": false,
                "FIELD_FOR_XREF": true,
                "FIELD_NAME": "UPDATED_AT",
                "FIELD_TRANS": "LOAD_TIME",
                "FIELD_TYPE": "TIMESTAMP_NTZ"
            }
        ]$$
    )
) S
ON D.TARGET_DATA = S.TARGET_DATA
WHEN MATCHED THEN
    UPDATE SET
        TARGET_LABEL = S.TARGET_LABEL,
        TARGET_TYPE = S.TARGET_TYPE,
        HISTORY_DATA = S.HISTORY_DATA,
        PROCESS_PRIORITY = S.PROCESS_PRIORITY,
        SCD_TYPE = S.SCD_TYPE,
        CTRL_FIELD = S.CTRL_FIELD,
        DATA_FIELD = S.DATA_FIELD,
        META_FIELD = S.META_FIELD
WHEN NOT MATCHED THEN 
    INSERT (
        TARGET_LABEL, 
        TARGET_TYPE, 
        TARGET_DATA, 
        HISTORY_DATA, 
        PROCESS_PRIORITY, 
        SCD_TYPE, 
        CTRL_FIELD,
        DATA_FIELD, 
        META_FIELD
    )
    VALUES (
        S.TARGET_LABEL, 
        S.TARGET_TYPE, 
        S.TARGET_DATA, 
        S.HISTORY_DATA, 
        S.PROCESS_PRIORITY, 
        S.SCD_TYPE, 
        S.CTRL_FIELD,
        S.DATA_FIELD, 
        S.META_FIELD 
    );



 /********************************************************************
 ** Schema Update Manually
 ********************************************************************/
USE SCHEMA ASL._METADATA;
/*
CALL CTRL_TASK_SCHEDULER('DIM', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('DIM', 'WORK');
CALL CTRL_TASK_SCHEDULER('FACT', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('FACT', 'WORK');
*/
