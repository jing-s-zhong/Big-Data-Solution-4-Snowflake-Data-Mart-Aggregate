-- 
-- default database should be ASL or ANALYTICS
--
USE DATABASE ASL;

/********************************************************************
 ** Data Mart Load Target Configuration 
 ********************************************************************/
--
-- Create warehouse target config data
--
USE SCHEMA _METADATA;
/*
TRUNCATE TABLE CTRL_SOURCE;
*/

MERGE INTO CTRL_SOURCE D
USING (
    SELECT TC.TARGET_ID,
        SOURCE_LABEL,
        SOURCE_DATA,
        SOURCE_ENABLED,
        FIELD_MAP,
        TRANSFORMATION
    FROM (
        SELECT $1 TARGET_DATA,
            $2 SOURCE_LABEL,
            $3 SOURCE_DATA,
            $4 SOURCE_ENABLED,
            PARSE_JSON($5) FIELD_MAP,
            $6 TRANSFORMATION
        FROM VALUES 
        (
            'ANALYSIS.DIM_PERSON',
            'HST.ONTOLOGY.PERSON',
            'HST.ONTOLOGY.PERSON',
            TRUE,
            $${}$$,
            NULL
        ),
        (
            'ANALYSIS.FACT_CONTACT',
            'HST.ONTOLOGY.PERSON_CONTACT',
            'HST.ONTOLOGY.PERSON_CONTACT',
            TRUE,
            $${}$$,
            NULL
        )
    ) SC
    JOIN _METADATA.CTRL_TARGET TC
    ON SC.TARGET_DATA = TC.TARGET_DATA
) S
ON D.SOURCE_DATA = S.SOURCE_DATA
WHEN MATCHED THEN
    UPDATE SET
        TARGET_ID = S.TARGET_ID,
        SOURCE_LABEL = S.SOURCE_LABEL,
        SOURCE_DATA = S.SOURCE_DATA,
        SOURCE_ENABLED = S.SOURCE_ENABLED,
        FIELD_MAP = S.FIELD_MAP,
        TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN 
    INSERT (
        TARGET_ID, 
        SOURCE_LABEL, 
        SOURCE_DATA, 
        SOURCE_ENABLED, 
        FIELD_MAP, 
        TRANSFORMATION
    )
    VALUES (
        S.TARGET_ID, 
        S.SOURCE_LABEL, 
        S.SOURCE_DATA, 
        S.SOURCE_ENABLED, 
        S.FIELD_MAP, 
        S.TRANSFORMATION
    );




 /********************************************************************
 ** Populate Data Marts Manually
 ********************************************************************/
USE SCHEMA ASL._METADATA;

CALL CTRL_TASK_SCHEDULER('DIM', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('DIM', 'WORK');
CALL CTRL_TASK_SCHEDULER('FACT', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('FACT', 'WORK');
