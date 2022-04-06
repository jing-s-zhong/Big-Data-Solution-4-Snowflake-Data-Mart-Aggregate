-- 
-- default database should be ASL or ANALYTICS
--
USE DATABASE ASL;

/********************************************************************
 ** Data Mart Solution Deployment Section
 ********************************************************************/
DROP SCHEMA IF EXISTS _METADATA;
CREATE SCHEMA IF NOT EXISTS _METADATA;
USE SCHEMA _METADATA;

--
-- Create a surogate key generator
--
DROP SEQUENCE IF EXISTS SG_KEY_GEN;
CREATE OR REPLACE SEQUENCE SG_KEY_GEN;
SELECT _METADATA.SG_KEY_GEN.NEXTVAL;

--
-- Create the metadata contorl objects
--
DROP TABLE IF EXISTS CTRL_TARGET;
CREATE OR REPLACE TABLE CTRL_TARGET
(
	TARGET_ID                       NUMBER NOT NULL IDENTITY,
	TARGET_LABEL					TEXT,
	TARGET_TYPE                     VARCHAR,
	TARGET_DATA					    TEXT NOT NULL,
	HISTORY_DATA                    VARCHAR,
	PROCESS_PRIORITY                VARCHAR,
	SCD_TYPE                        NUMBER,
	CTRL_FIELD		    		    VARIANT,
	DATA_FIELD		    		    VARIANT,
	META_FIELD		    		    VARIANT,
	--GROUPBY_PATTERN		    		NUMBER,
	--GROUPBY_FLEXIBLE				BOOLEAN,
	--AGGREGATE_COLUMNS				ARRAY,
	--AGGREGATE_FUNCTIONS				ARRAY,
	--BATCH_CONTROL_COLUMN			TEXT,
	--BATCH_CONTROL_SIZE				NUMBER,
	--BATCH_CONTROL_NEXT				TEXT,
	--BATCH_PROCESSED		    		TIMESTAMP_NTZ,
	--BATCH_PROCESSING				TIMESTAMP_NTZ,
	--BATCH_MICROCHUNK_CURRENT 	    TIMESTAMP_NTZ,
	--BATCH_SCHEDULE_TYPE				TEXT,
	--BATCH_SCHEDULE_LAST				TIMESTAMP_NTZ,
	CONSTRAINT PK_CTRL_TARGET PRIMARY KEY (TARGET_ID)
);

DROP TABLE IF EXISTS CTRL_SOURCE;
CREATE OR REPLACE TABLE CTRL_SOURCE
(
    TARGET_ID                       NUMBER NOT NULL,
    SOURCE_ID                       NUMBER NOT NULL IDENTITY,
	SOURCE_LABEL					TEXT,
	SOURCE_DATA	        		    TEXT NOT NULL,
    --SOURCE_QUERY                    VARIANT,
	SOURCE_ENABLED	        	    BOOLEAN,
	FIELD_MAP		    		    VARIANT,
	--PATTERN_DEFAULT	        	    NUMBER,
	--PATTERN_FLEXIBLE	    		BOOLEAN,
	--DATA_AVAILABLETIME	    	    TIMESTAMP_NTZ,
	--DATA_CHECKSCHEDULE	    	    TIMESTAMP_NTZ,
	TRANSFORMATION	        	    TEXT,
	CONSTRAINT PK_CTRL_SOURCE PRIMARY KEY (SOURCE_ID),
	CONSTRAINT FK_CTRL_SOURCE_CTRL_TARGET FOREIGN KEY (TARGET_ID)
		REFERENCES CTRL_TARGET(TARGET_ID)
);

--
-- create a logging table
--
DROP TABLE IF EXISTS CTRL_LOG;
CREATE OR REPLACE TABLE CTRL_LOG
(
	EVENT_ID NUMBER NOT NULL IDENTITY,
	EVENT_TIME TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
	EVENT_STATUS TEXT,
	EVENT_MESSAGE TEXT,
	EVENT_QUERY TEXT
);

--
-- create a data loading query generator
--
DROP VIEW IF EXISTS CTRL_TASK_SCHEDULE;
CREATE OR REPLACE VIEW CTRL_TASK_SCHEDULE AS
/*
SELECT * --, Func_i(*) Expression_i
FROM CTRL_TARGET T
JOIN CTRL_SOURCE S
ON T.TARGET_ID = S.TARGET_ID
*/
WITH SQL_SYNTAX_TEMPLATES AS ( -- used SQL syntax templates, TEMPLATE_ID controls execution priority from large to small
SELECT $1 TEMPLATE_ID, $2 TEMPLATE_TEXT
FROM VALUES
(1, '
MERGE INTO {{TARGET_DATA}} D 
USING (
    WITH SOURCE_TRANSED AS (
        SELECT {{SOURCE_SELECT_LIST}}
        FROM {{SOURCE_DATA}}
        --WHERE DIGEST != ''DELETE'' /* TODO: Teak digest view and transformation query to enable "DELETE" action */
    )
    SELECT {{DATA_SELECT_LIST}}
    FROM SOURCE_TRANSED
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} AND D.{{VALID_TO_FIELD}} IS NULL
WHEN MATCHED AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}} THEN 
UPDATE SET 
	{{DATA_UPDATE_LIST}}
WHEN NOT MATCHED THEN 
INSERT (
	{{DATA_FIELD_LIST}}
) 
VALUES (
	{{DATA_VALUE_LIST}}
);'),
(2, '
MERGE INTO {{TARGET_DATA}} D 
USING (
    WITH SOURCE_TRANSED AS (
        SELECT {{SOURCE_SELECT_LIST}}
        FROM {{SOURCE_DATA}}
        --WHERE DIGEST != ''INSERT'' /* TODO: Teak digest view and transformation query to enable "INSERT" action */
    )
    SELECT {{DATA_SELECT_LIST}}
    FROM SOURCE_TRANSED
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} AND D.{{VALID_TO_FIELD}} IS NULL
AND D.{{VALID_FROM_FIELD}} < S.{{DATA_TIME_FIELD}}::DATE - 1
WHEN MATCHED AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}} THEN 
UPDATE SET {{VALID_TO_FIELD}} = S.{{DATA_TIME_FIELD}}::DATE - 1
;'),
(3, '
INSERT INTO {{HISTORY_DATA}} 
SELECT D.*
FROM {{TARGET_DATA}} D 
JOIN (
    WITH SOURCE_TRANSED AS (
        SELECT {{SOURCE_SELECT_LIST}}
        FROM {{SOURCE_DATA}}
        --WHERE DIGEST != ''INSERT'' /* TODO: Teak digest view and transformation query to enable "INSERT" action */
    )
    SELECT {{DATA_SELECT_LIST}}
    FROM SOURCE_TRANSED
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} AND D.{{VALID_TO_FIELD}} IS NULL
AND D.{{VALID_FROM_FIELD}} < S.{{DATA_TIME_FIELD}}::DATE - 1
AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}}
;'),
(4, '
MERGE INTO {{TARGET_DATA}} D 
USING (
    WITH SOURCE_TRANSED AS (
        SELECT {{SOURCE_SELECT_LIST}}
        FROM {{SOURCE_DATA}}
        --WHERE DIGEST != ''INSERT'' /* TODO: Teak digest view and transformation query to enable "INSERT" action */
    )
    SELECT {{DATA_SELECT_LIST}}
    FROM SOURCE_TRANSED
) S 
ON D.{{DATA_KEY_FIELD}} = S.{{DATA_KEY_FIELD}} 
WHEN MATCHED AND D.{{DATA_HASH_FIELD}} != S.{{DATA_HASH_FIELD}} THEN 
UPDATE SET {{HISTORY_FIELD}} = ARRAY_CAT({{HISTORY_FIELD}}, OBJECT_CONSTRUCT(S.*)),
	{{DATA_UPDATE_LIST}}
WHEN NOT MATCHED THEN 
INSERT (
    {{HISTORY_FIELD}},
	{{DATA_FIELD_LIST}}
) 
VALUES (
    OBJECT_CONSTRUCT(S.*),
	{{DATA_VALUE_LIST}}
);')
),
JINJA_TEMPLATE AS ( /* State Machine */
    SELECT T.TARGET_ID,
        T.TARGET_TYPE,
        T.TARGET_DATA,
        T.HISTORY_DATA,
        T.CTRL_FIELD,
        T.SCD_TYPE,
        S.TEMPLATE_ID,
        S.TEMPLATE_TEXT
    FROM (
        SELECT *,
            CASE SCD_TYPE
                WHEN 1 THEN ARRAY_CONSTRUCT(1) 
                WHEN 2 THEN ARRAY_CONSTRUCT(2, 1) 
                WHEN 3 THEN ARRAY_CONSTRUCT(4) 
                WHEN 4 THEN ARRAY_CONSTRUCT(3, 1) 
                WHEN 5 THEN ARRAY_CONSTRUCT(3, 1) 
                WHEN 6 THEN ARRAY_CONSTRUCT(4, 2, 1) 
            END TEMP_COMPS
        FROM CTRL_TARGET
    ) T
    JOIN SQL_SYNTAX_TEMPLATES S
    ON ARRAY_CONTAINS( S.TEMPLATE_ID::VARIANT, T.TEMP_COMPS)
),
JINJA_EXPPRESSION AS ( /* Status Conditions */
    SELECT TARGET_ID,
        PROCESS_PRIORITY,
        SOURCE_DATA,
        IFNULL(CTRL_FIELD:DATA_KEY_FIELD,  'DATA_KEY') DATA_KEY_FIELD,
        IFNULL(CTRL_FIELD:DATA_HASH_FIELD, 'DATA_HASH') DATA_HASH_FIELD,
        IFNULL(CTRL_FIELD:DATA_TIME_FIELD, 'DATA_TIME') DATA_TIME_FIELD,
        IFNULL(CTRL_FIELD:VALID_FROM_FIELD, 'VALID_FROM') VALID_FROM_FIELD,
        IFNULL(CTRL_FIELD:VALID_TO_FIELD, 'VALID_TO') VALID_TO_FIELD,
        ARRAY_TO_STRING(ARRAY_AGG(TARGET_FIELD), ', \n\t') DATA_FIELD_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(DATA_VALUE), ', \n\t') DATA_VALUE_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(DATA_UPDATE), ', \n\t') DATA_UPDATE_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(SOURCE_SELECT), ', \n\t\t\t') SOURCE_SELECT_LIST,
        ARRAY_TO_STRING(ARRAY_AGG(DATA_SELECT), ', \n\t\t') DATA_SELECT_LIST
    FROM (
        SELECT T.TARGET_ID,
            T.TARGET_DATA,
            T.PROCESS_PRIORITY,
            IFF(IFNULL(S.TRANSFORMATION, '') = '', S.SOURCE_DATA, CONCAT ('(\n\t\t\t/* transformation begin */', S.TRANSFORMATION, '/* transformation end */\n\t\t)')) SOURCE_DATA,
            CTRL_FIELD,
            ARRAY_SIZE(T.DATA_FIELD) SOURCE_SIZE,
            VALUE:FIELD_NAME::VARCHAR TARGET_FIELD,
            CONCAT('{{S.}}', TARGET_FIELD) DATA_VALUE,
            CONCAT(TARGET_FIELD, ' = {{S.}}', TARGET_FIELD) DATA_UPDATE,
            S.FIELD_MAP[TARGET_FIELD] SOURCE_MAP,
            IFF(INDEX < SOURCE_SIZE, CONCAT(IFNULL(CONCAT(REPLACE(SOURCE_MAP:FIELD_TRANS::VARCHAR, '?', SOURCE_MAP:FIELD_NAME::VARCHAR),' '),''), TARGET_FIELD), NULL) SOURCE_SELECT,
            CONCAT(REPLACE(VALUE:FIELD_TRANS::VARCHAR,'?', VALUE:FIELD_NAME), ' ', VALUE:FIELD_NAME) DATA_SELECT
        FROM CTRL_TARGET T
        JOIN CTRL_SOURCE S
        ON T.TARGET_ID = S.TARGET_ID, 
        LATERAL FLATTEN ( INPUT => ARRAY_CAT(T.DATA_FIELD, T.META_FIELD) )
        WHERE UPPER(VALUE:FIELD_TRANS::VARCHAR) NOT IN ('IDENTITY', 'AUTOINCREMENT')
    )
    GROUP BY 1,2,3,4,5,6,7,8
)
SELECT TARGET_ID, TARGET_DATA, SOURCE_DATA, TARGET_TYPE, PROCESS_PRIORITY, SCD_TYPE, 
    ARRAY_AGG(JINJA_REDENDED) WITHIN GROUP (ORDER BY TEMPLATE_ID DESC) DATA_LOADER
FROM ( /* Reaction Truth Table */
    SELECT TEP.TARGET_ID,
        TEP.TARGET_TYPE,
        TEP.TARGET_DATA,
        TEP.HISTORY_DATA,
        EXP.PROCESS_PRIORITY,
        TEP.SCD_TYPE,
        EXP.SOURCE_DATA,
        TEP.TEMPLATE_ID,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TEP.TEMPLATE_TEXT,
            '{{SOURCE_SELECT_LIST}}',EXP.SOURCE_SELECT_LIST),
            '{{DATA_FIELD_LIST}}',EXP.DATA_FIELD_LIST),
            '{{DATA_SELECT_LIST}}',EXP.DATA_SELECT_LIST),
            '{{DATA_UPDATE_LIST}}',EXP.DATA_UPDATE_LIST),
            '{{DATA_VALUE_LIST}}',EXP.DATA_VALUE_LIST),
            '{{DATA_KEY_FIELD}}', DATA_KEY_FIELD),
            '{{DATA_HASH_FIELD}}', DATA_HASH_FIELD),
            '{{DATA_TIME_FIELD}}', DATA_TIME_FIELD),
            '{{VALID_FROM_FIELD}}', VALID_FROM_FIELD),
            '{{VALID_TO_FIELD}}', VALID_TO_FIELD),
            '{{SOURCE_DATA}}',EXP.SOURCE_DATA),
            '{{TARGET_DATA}}',TEP.TARGET_DATA),
            '{{HISTORY_DATA}}',TEP.HISTORY_DATA),
            '{{S.}}', 'S.') AS JINJA_REDENDED
    FROM JINJA_EXPPRESSION EXP
    JOIN JINJA_TEMPLATE TEP
    ON EXP.TARGET_ID = TEP.TARGET_ID
    )
GROUP BY 1,2,3,4,5,6
;


--
-- create the data load procedure
--
DROP PROCEDURE IF EXISTS CTRL_TASK_SCHEDULER(VARCHAR, VARCHAR);
CREATE OR REPLACE PROCEDURE CTRL_TASK_SCHEDULER (
    DATA_TYPE VARCHAR, -- DIM or FACT
    CALL_MODE VARCHAR -- WORK or DEBUG otherwise
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    var loggerQuery = 'INSERT INTO CTRL_LOG (EVENT_STATUS, EVENT_MESSAGE, EVENT_QUERY) VALUES(:1, :2, :3);';
    var loggerStatus = '', loggerMessage = '', loggerMode = 'VERBOSE', result = '';
    try {
        var scheduleQuery = 'SELECT *, CURRENT_SCHEMA() METADATA_SCHEMA FROM CTRL_TASK_SCHEDULE WHERE TARGET_TYPE = :1 ORDER BY PROCESS_PRIORITY;';
        var scheduleStmnt = snowflake.createStatement ({ sqlText: scheduleQuery, binds:[DATA_TYPE] });
        var scheduleReslt = scheduleStmnt.execute();
        //result += scheduleQuery + '\n';
        while (scheduleReslt.next()) {
            var metadataSchema = scheduleReslt.getColumnValue("METADATA_SCHEMA");
            var metadataScript = scheduleReslt.getColumnValue("DATA_LOADER");
            //result += JSON.stringify(metadataScript) + '\n';
            for (var i = 0; i < metadataScript.length; i++) {
                var loaderDML = metadataScript[i];
                var loaderCMD = snowflake.createStatement ({ sqlText: loaderDML });
                if (CALL_MODE == "WORK") {
                    try {
                        var loaderRET = loaderCMD.execute();
                        loggerStatus = 'SUCCEDDED';
                    }
                    catch (err1) {
                        loggerStatus = 'FAILED';
                        loggerMSG = err1.toString();
                    }
                    finally {
                        if (loggerMode == 'ERROR_ONLY' && loggerStatus == 'SUCCEDDED') continue;
                        var loggerStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, loaderDML]});
                        var loggerRsult = loggerStmnt.execute();
                    }
                }
                result += loaderDML + '\n';
            }
        }
    }
    catch (err) {
        result += err.toString() + '\n';
        loggerStatus = 'FAILED';
        loggerMessage = err1.toString();
        var loggerStmnt = snowflake.createStatement ({ sqlText: loggerQuery, binds: [loggerStatus, loggerMessage, "CALL CTRL_TASK_SCHEDULER(...)"]});
        var loggerRsult = loggerStmnt.execute();
    }
    return result;
$$;

/*
CALL CTRL_TASK_SCHEDULER('DIM', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('DIM', 'WORK');
CALL CTRL_TASK_SCHEDULER('FACT', 'DEBUG');
CALL CTRL_TASK_SCHEDULER('FACT', 'WORK');
*/


/********************************************************************
 ** Data Mart Solution Automation Setup
 ********************************************************************/
/*
--
-- create a snow job to schedule the loader
--
CREATE OR REPLACE TASK RUN_CTRL_SCHEDULER
  WAREHOUSE = ANALTICS_WH
  SCHEDULE = 'USING CRON 0 0/2 * * * UTC'
AS
CALL CTRL_SCHEDULER();

--
-- enable or disable the snow job schedule;
--
ALTER TASK RUN_CTRL_SCHEDULER RESUME;
ALTER TASK RUN_CTRL_SCHEDULER SUSPEND;
*/
