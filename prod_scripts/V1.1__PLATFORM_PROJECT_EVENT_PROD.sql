--------------------------------------------------------------------------------------------------------------
-------------------------------------1. PLATFORM_PROJECT_EVENT------------------------------------------------




create or replace table  "UNITY_PROD_DB"."UNITY_STAGING"."LOG"  like  "UNITY_DEV_DB"."UNITY_STAGING"."LOG" ;

create or replace table "UNITY_PROD_DB"."UNITY_STAGING"."STG_PLATFORM_PROJECT_EVENT"  like  "UNITY_DEV_DB"."UNITY_STAGING"."STG_PLATFORM_PROJECT_EVENT" ;

create or replace table  "UNITY_PROD_DB"."UNITY_ARCHIVE"."STG_PLATFORM_PROJECT_EVENT_ARCHIVE" like  "UNITY_DEV_DB"."UNITY_ARCHIVE"."STG_PLATFORM_PROJECT_EVENT_ARCHIVE"  ;

create or replace table  "UNITY_PROD_DB"."UNITY_CORE"."PLATFORM_PROJECT_EVENT"  like  "UNITY_DEV_DB"."UNITY_CORE"."PLATFORM_PROJECT_EVENT" ;


CREATE OR REPLACE PROCEDURE UNITY_CORE.LOAD_DW_PLATFORM_PROJECT_EVENT_SPROC(param VARCHAR)
		RETURNS VARCHAR(16777216)
		LANGUAGE JAVASCRIPT
		EXECUTE AS CALLER
		AS
		$$
		var procedure_name = 'LOAD_DW_PLATFORM_PROJECT_EVENT_SPROC';
		var p = PARAM;
		var PROD = 'UNITY_PROD_DB';
		var DEV = 'UNITY_DEV_DB';
		var TEST = 'UNITY_TEST_DB';

		if (p == 'PROD') 
		{ 
        
			var res1=snowflake.execute( {sqlText:`USE ${PROD}`} );
		}
		else if(p == 'DEV' )
		{  
			var res2=snowflake.execute( {sqlText:`USE ${DEV}`} );
		}
		else if(p == 'TEST' )
		{  
			var res3=snowflake.execute( {sqlText:`USE ${TEST}`} );
		}
		
		else
		{ 
			throw "Invalid DATABASE Name.USE DEV FOR UNITY_DEV_DB AND PROD FOR UNITY_PROD_DB ";
			//return "Invalid DATABASE Name.USE DEV FOR UNITY_DEV_DB AND PROD FOR UNITY_PROD_DB ";
		}
		
		try 
		{   
			
			var timestp = snowflake.execute( {sqlText:` 
            SELECT CURRENT_TIMESTAMP()::string;
            `} );
            
            timestp.next();
            var res_timestp = timestp.getColumnValue(1);
			
			var res_archive = snowflake.execute( {sqlText:` 
            INSERT INTO UNITY_ARCHIVE.STG_PLATFORM_PROJECT_EVENT_ARCHIVE
            SELECT * FROM UNITY_STAGING.STG_PLATFORM_PROJECT_EVENT
			WHERE DW_CREATED_DATE <= (select to_timestamp_ntz('${res_timestp}'));
            `} );
            
            var i_count = snowflake.execute( {sqlText:` 
            SELECT COUNT(*) FROM UNITY_CORE.PLATFORM_PROJECT_EVENT;
            `} );
            
            i_count.next();
            var result = "PLATFORM_PROJECT_EVENT: INITIAL COUNT = " + i_count.getColumnValue(1);

			var res_merge_ppe=snowflake.execute( {sqlText:`MERGE INTO UNITY_CORE.PLATFORM_PROJECT_EVENT  TGT
			USING  ( select * from(
			select A.*,ROW_NUMBER()over( partition by A.id order by DW_CREATED_DATE desc,KAFKA_CREATED_AT  desc) as rn from (SELECT * FROM UNITY_STAGING.STG_PLATFORM_PROJECT_EVENT WHERE DW_CREATED_DATE <= (select to_timestamp_ntz('${res_timestp}')))  A
			)where rn=1) AS SRC
			ON (TGT.ID = SRC.ID)
			WHEN NOT MATCHED THEN
			INSERT 
			(ID,OFFSET,KAFKA_CREATED_AT ,CREATED_AT,UPDATED_AT,EVENT_TYPE,PARENT_ID,CLOCK,PROJECT_ID,CONTRACT_ID,SERVICE_PROVIDER_ID,DEPOSIT_PERCENT,MINIMUM_FEE,CURRENCY,CREATED_BY,SECTORS,
			STATE,VERSION,PAYMENT_STRATEGY,CONTRACT_STRATEGY,FILE_IDS,NAME,APPROVAL_STRATEGY,TUTORIAL_TYPE,PROJECT_EVENT_BATCH_ID,PROJECT_ROLES,
			DESCRIPTION,AWARD_STRATEGY,LEGAL_STRATEGY,IS_PANEL,PROJECT_SET_CHANGE,LOCALE,EXTERNAL_SYSTEMS,IS_SOLE_SOURCE,DW_CREATED_DATE,DW_UPDATED_DATE)
			VALUES
			(SRC.ID,SRC.OFFSET,SRC.KAFKA_CREATED_AT ,SRC.CREATED_AT,SRC.UPDATED_AT,SRC.EVENT_TYPE,SRC.PARENT_ID,SRC.CLOCK,SRC.PROJECT_ID,SRC.CONTRACT_ID,SRC.SERVICE_PROVIDER_ID,SRC.DEPOSIT_PERCENT,SRC.MINIMUM_FEE,
			SRC.CURRENCY,SRC.CREATED_BY,SRC.SECTORS,SRC.STATE,SRC.VERSION,SRC.PAYMENT_STRATEGY,SRC.CONTRACT_STRATEGY,SRC.FILE_IDS,SRC.NAME,SRC.APPROVAL_STRATEGY,
			SRC.TUTORIAL_TYPE,SRC.PROJECT_EVENT_BATCH_ID,SRC.PROJECT_ROLES,SRC.DESCRIPTION,SRC.AWARD_STRATEGY,
			SRC.LEGAL_STRATEGY,SRC.IS_PANEL,SRC.PROJECT_SET_CHANGE,SRC.LOCALE,SRC.EXTERNAL_SYSTEMS,SRC.IS_SOLE_SOURCE,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())
			WHEN MATCHED THEN UPDATE SET 
			TGT.ID= SRC.ID,
			TGT.OFFSET=SRC.OFFSET,
			TGT.KAFKA_CREATED_AT=SRC.KAFKA_CREATED_AT,
			TGT.CREATED_AT=SRC.CREATED_AT,
			TGT.UPDATED_AT=SRC.UPDATED_AT,
			TGT.EVENT_TYPE= SRC.EVENT_TYPE,
			TGT.PARENT_ID= SRC.PARENT_ID,
			TGT.CLOCK= SRC.CLOCK,
			TGT.PROJECT_ID= SRC.PROJECT_ID,
			TGT.CONTRACT_ID= SRC.CONTRACT_ID,
			TGT.SERVICE_PROVIDER_ID= SRC.SERVICE_PROVIDER_ID,
			TGT.DEPOSIT_PERCENT= SRC.DEPOSIT_PERCENT,
			TGT.MINIMUM_FEE= SRC.MINIMUM_FEE,
			TGT.CURRENCY= SRC.CURRENCY,
			TGT.CREATED_BY= SRC.CREATED_BY,
			TGT.SECTORS= SRC.SECTORS,
			TGT.STATE= SRC.STATE,
			TGT.VERSION= SRC.VERSION,
			TGT.PAYMENT_STRATEGY= SRC.PAYMENT_STRATEGY,
			TGT.CONTRACT_STRATEGY= SRC.CONTRACT_STRATEGY,
			TGT.FILE_IDS= SRC.FILE_IDS,
			TGT.NAME= SRC.NAME,
			TGT.APPROVAL_STRATEGY= SRC.APPROVAL_STRATEGY,
			TGT.TUTORIAL_TYPE= SRC.TUTORIAL_TYPE,
			TGT.PROJECT_EVENT_BATCH_ID= SRC.PROJECT_EVENT_BATCH_ID,
			TGT.PROJECT_ROLES= SRC.PROJECT_ROLES,
			TGT.DESCRIPTION= SRC.DESCRIPTION,
			TGT.AWARD_STRATEGY= SRC.AWARD_STRATEGY,
			TGT.LEGAL_STRATEGY= SRC.LEGAL_STRATEGY,
			TGT.IS_PANEL= SRC.IS_PANEL,
			TGT.PROJECT_SET_CHANGE= SRC.PROJECT_SET_CHANGE,
			TGT.LOCALE= SRC.LOCALE,
			TGT.EXTERNAL_SYSTEMS= SRC.EXTERNAL_SYSTEMS,
			TGT.IS_SOLE_SOURCE= SRC.IS_SOLE_SOURCE,
			TGT.DW_UPDATED_DATE=CURRENT_TIMESTAMP();`} );
	
            
            var f_count = snowflake.execute( {sqlText:` 
            SELECT COUNT(*) FROM UNITY_CORE.PLATFORM_PROJECT_EVENT;
            `} );
            
            f_count.next();
            result += " FINAL COUNT = " + f_count.getColumnValue(1);
            
            res_merge_ppe.next();
            result += "\nROWS INSERTED: " + res_merge_ppe.getColumnValue("number of rows inserted") +" ROWS UPDATED: " + res_merge_ppe.getColumnValue("number of rows updated") ;

			var res_delete = snowflake.execute( {sqlText:` 
            DELETE FROM UNITY_STAGING.STG_PLATFORM_PROJECT_EVENT
			WHERE DW_CREATED_DATE <= (select to_timestamp_ntz('${res_timestp}'));
            `} );
            
            result=result.replace(/'/g,"''");
			var res_log=snowflake.execute( {sqlText:`INSERT INTO UNITY_STAGING.LOG(PROCEDURE_NAME, PARAMETER_PASSED, RESULT_LOG, DATE_TIME) VALUES
														  ('${procedure_name}','${p}','${result}',CURRENT_TIMESTAMP)`} );
			return result;
            
		  }
		catch(err)
		{ 
			result =  " Failed: Code:  ERROR_1 WHILE MERGING THE DATA " + err.code + "\n  State: " + err.state;
			result += "\n  Message: " + err.message;
			result += "\nStack Trace:\n" + err.stackTraceTxt; 
            result=result.replace(/'/g,"''");

	        try
			{
				  var res_log=snowflake.execute( {sqlText:`INSERT INTO UNITY_STAGING.LOG(PROCEDURE_NAME, PARAMETER_PASSED, RESULT_LOG, DATE_TIME) VALUES
														  ('${procedure_name}','${p}','${result}',CURRENT_TIMESTAMP)`} );
			}
			catch(e)
			{
                result +=  "\n "+"Failed: Code:  ERROR_2 while inserting data to LOG table  " + e.code + "\n  State: " + e.state;
				result += "\n  Message: " + e.message;
				result += "\nStack Trace:\n" + e.stackTraceTxt;
			}
			throw result;
           
		}
		$$;
        
        
--  CALL UNITY_CORE.LOAD_DW_PLATFORM_PROJECT_EVENT_SPROC('PROD');


CREATE OR REPLACE TASK UNITY_CORE.TASK_PLATFORM_PROJECT_EVENT
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 0,6,12,18 * * * America/Los_Angeles'
AS
CALL UNITY_CORE.LOAD_DW_PLATFORM_PROJECT_EVENT_SPROC('PROD');