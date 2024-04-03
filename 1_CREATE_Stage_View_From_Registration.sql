---- BEGIN DYNAMIC SQL FOR VIEW CREATION ---

-- This script is used to create a staging view from information registered in a stage.view_registration table and information schema
-- A precondition is that a metadata driven ADF pipeline has been created to pull data into a schema from a source ("SourceA") and that the business and row keys have been identified and stored in the view_registration table
-- The dynamic SQL walks the registered tables and creates a view for each with hash keys that can be used for change data capture
-- Because it is dynamic it can create or script the creation of all views for the staged data at one time
-- Additional views and tables are created in subsequent scripts to isolate the current version of a row and store all changes incrementally over time, replicating a "poor man's" delta table in a dedicated SQL Pool in Azure Synapse


DECLARE 
  @TABLE_SCHEMA NVARCHAR(200)
, @TABLE_NAME NVARCHAR(200)
, @BK_LIST NVARCHAR(500)
, @RK_LIST NVARCHAR(500)
, @i int = 1
, @t int = 1
, @rowcount int
, @sql1 NVARCHAR(MAX)
, @sql2 NVARCHAR(MAX)
, @sql3 NVARCHAR(MAX)
, @sql4 NVARCHAR(MAX)
, @sqlGO NVARCHAR(MAX)
, @STAGE_TABLE_COLUMN_LIST NVARCHAR(MAX)

BEGIN TRY


	SET @sqlGO = CHAR(10) +'GO' + CHAR(10) 

	-- Create Temp Table to iterate over as Synapse does not support cursors
	CREATE TABLE #TBL WITH (DISTRIBUTION = ROUND_ROBIN) AS
		SELECT -- TOP 10
			ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS IX_SEQUENCE
			, SOURCE_SCHEMA
			, TABLE_NAME
			, BUSINESS_KEY_COLUMN_LIST
			, ROW_KEY_COLUMN_LIST
		FROM Stage.View_Registration 
		WHERE 
		SOURCE_SCHEMA = 'SourceA'
		
	-- Get total row count from temp table for looping
	SELECT @rowcount = COUNT(*) FROM #TBL;

	PRINT @rowcount

	SELECT * FROM #TBL ORDER BY TABLE_NAME

	-- Loop over table list to create views
	WHILE @i <= @rowcount

		BEGIN

			SELECT @TABLE_SCHEMA = SOURCE_SCHEMA, @TABLE_NAME =  TABLE_NAME, @BK_LIST = isnull(BUSINESS_KEY_COLUMN_LIST,''''''),  @RK_LIST = isnull(Row_Key_Column_List,'''''') FROM #tbl WHERE IX_SEQUENCE = @i
		
			-- STANDARD COLUMN LIST
			SELECT @STAGE_TABLE_COLUMN_LIST = STRING_AGG('['+COLUMN_NAME+']',', ') FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TABLE_NAME AND TABLE_SCHEMA = @TABLE_SCHEMA
									
			PRINT '-- Operating on: ' + @TABLE_SCHEMA +'.'+@TABLE_NAME
			PRINT 'BUSINESS KEY LIST:  ' + ISNULL(@BK_LIST, 'NULL') 
			PRINT 'ROW KEY LIST: ' + ISNULL(@RK_LIST,NULL)
			PRINT 'COLUMN LIST: ' + ISNULL(@STAGE_TABLE_COLUMN_LIST,'NULL')
			PRINT ''

			IF LEN(@BK_LIST) > 0
				BEGIN
				
					SET @sql1 = CHAR(10) + 'Drop View Stage.' + @TABLE_SCHEMA + '_' + @TABLE_NAME + '_VW' 
				
					SET @sql2 = 'Create View Stage.' + @TABLE_SCHEMA + '_' + @TABLE_NAME + '_VW AS 
					SELECT '
					SET @sql2 = @sql2 + '''' + @TABLE_SCHEMA + ''' AS SOURCE_SCHEMA, ''' + @TABLE_NAME + ''' AS SOURCE_TABLE, HASHBYTES(''SHA1'', CONCAT_WS('','',''' + @TABLE_NAME + ''', ' + @BK_LIST + ')) AS BK_HASH, HASHBYTES(''SHA1'', CONCAT_WS('','',''' + @TABLE_NAME + ''', ' + @RK_List + ')) AS RK_HASH, GETDATE() AS STAGE_DATE, HASHBYTES(''SHA1'', CONCAT_WS('','',''' + @TABLE_NAME + ''', ' 
					SET @sql3 = ')) AS ROW_HASH, 0 AS IS_DELETED, ' 
					set @sql4 = ' FROM '+ @TABLE_SCHEMA + '.' + @TABLE_NAME
			    
					PRINT isnull(@sql1, 'no SQL')
					PRINT isnull(@sqlGO, 'no SQL')
					PRINT isnull(@sql2, 'no SQL')
					PRINT isnull(@STAGE_TABLE_COLUMN_LIST, 'no SQL')
					PRINT isnull(@sql3, 'no SQL')
					PRINT isnull(@STAGE_TABLE_COLUMN_LIST, 'no SQL')
					PRINT isnull(@sql4, 'no SQL')
					PRINT isnull(@sqlGO, 'no SQL')
					--PRINT ''
					--EXECUTE sp_executesql @sql

				END
			ELSE
				BEGIN

					PRINT '***** NO KEYS, NO VIEW *****'
			
				END

			SET @i = @i+1

		END
END TRY

BEGIN CATCH 

	PRINT ERROR_NUMBER() 
	PRINT ERROR_MESSAGE()

END CATCH

DROP TABLE #TBL