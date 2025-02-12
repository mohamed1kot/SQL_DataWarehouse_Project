/*
==============================================================================
Stored Procedure: Load_DataWarehouse
Purpose:
    This stored procedure is designed to load data from CRM and ERP systems 
    (stored as CSV files) into the Bronze layer of a Data Warehouse. 
    It handles the data ingestion process, including table truncation, 
    bulk insertion, and tracking execution durations.
==============================================================================
Key Features:
1. **Source to Bronze Layer Transition**:
    - Moves raw data from the source system into the Bronze layer of the Data Warehouse.
    - The Bronze layer represents the raw, unprocessed version of the data.

2. **CRM Tables Processing**:
    - Truncates and reloads `CRM_Cust_info`, `CRM_prd_info`, and `CRM_sales_details` tables.

3. **ERP Tables Processing**:
    - Truncates and reloads `ERP_cust_az12` and `ERP_loc_a101` tables.

4. **Execution Tracking**:
    - Calculates and prints the duration of loading operations for each table.
    - Logs the total time taken to complete the process for the entire Bronze layer.

5. **Error Handling**:
    - Uses a `TRY...CATCH` block to handle errors gracefully during execution.
    - Displays error details including message, number, and state in case of failure.

Steps in the Procedure:
    a. **Initialize Variables**:
        - File paths for CRM and ERP CSV datasets are set.
        - Start and end times for measuring load durations are initialized.
    
    b. **CRM Tables Loading**:
        - Each CRM table is truncated to remove old data.
        - Bulk Insert commands load new data from corresponding CSV files.
        - Execution duration for each table is calculated and printed.

    c. **ERP Tables Loading**:
        - Similar process to CRM tables: truncate, bulk insert, and calculate execution duration.

    d. **Batch Processing Summary**:
        - Prints the total time taken for the entire batch operation.

    e. **Error Handling**:
        - Catches and logs any errors that occur during the process.

==============================================================================
*/


CREATE OR ALTER PROCEDURE Bronze.Load_DataWarehouse
AS
BEGIN
    -- Declare file path variables
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    DECLARE @query NVARCHAR(MAX);
    DECLARE @source_erp NVARCHAR(255);
    DECLARE @source_crm NVARCHAR(255);

	SET @source_crm = 'C:\Users\abdal\Desktop\sql_DataWarehouse_Project\datasets\source_crm\';
    SET @source_erp = 'C:\Users\abdal\Desktop\sql_DataWarehouse_Project\datasets\source_erp\';
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT '       >>> Loading Bronze Layer.....';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT '       >>> Loading CRM Tables.....';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.CRM_Cust_info"';
		TRUNCATE TABLE DataWarehouse.Bronze.CRM_Cust_info;

		PRINT 'Loading Table: "Bronze.CRM_Cust_info"';
		SET @query = '
		BULK INSERT DataWarehouse.Bronze.CRM_Cust_info
		FROM ''' + @source_crm + 'cust_info.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.CRM_prd_info"';
		TRUNCATE TABLE DataWarehouse.Bronze.CRM_prd_info;
		PRINT 'Loading Table: "Bronze.CRM_prd_info"';

		SET @query = '
		BULK INSERT DataWarehouse.Bronze.CRM_prd_info
		FROM ''' + @source_crm + 'prd_info.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.CRM_sales_details"';
		TRUNCATE TABLE DataWarehouse.Bronze.CRM_sales_details;

		PRINT 'Loading Table: "Bronze.CRM_sales_details"';
		SET @query = '
		BULK INSERT DataWarehouse.Bronze.CRM_sales_details
		FROM ''' + @source_crm + 'sales_details.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.ERP_cust_az12"';
		TRUNCATE TABLE DataWarehouse.Bronze.ERP_cust_az12;

		PRINT 'Loading Table: "Bronze.ERP_cust_az12"';
		SET @query = '
		BULK INSERT DataWarehouse.Bronze.ERP_cust_az12
		FROM ''' + @source_erp + 'CUST_AZ12.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.ERP_loc_a101"';
		TRUNCATE TABLE DataWarehouse.Bronze.ERP_loc_a101;

		PRINT 'Loading Table: "Bronze.ERP_loc_a101"';
		SET @query = '
		BULK INSERT DataWarehouse.Bronze.ERP_loc_a101
		FROM ''' + @source_erp + 'LOC_A101.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Bronze.ERP_px_cat_g1v2"';
		TRUNCATE TABLE DataWarehouse.Bronze.ERP_px_cat_g1v2;

		PRINT 'Loading Table: "Bronze.ERP_px_cat_g1v2"';
		SET @query = '
		BULK INSERT DataWarehouse.Bronze.ERP_px_cat_g1v2
		FROM ''' + @source_erp + 'PX_CAT_G1V2.csv'''
		+ '
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = '','',
			TABLOCK
		)';
		EXEC(@query);

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer is Completed in: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second';
		PRINT '==============================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END