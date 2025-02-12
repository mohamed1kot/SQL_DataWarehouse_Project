/*
This stored procedure, Silver.Load_DataWarehouse, is designed to load data from the Bronze layer
into the Silver layer of the Data Warehouse. It follows an ETL (Extract, Transform, Load) process
with error handling and logging.

### Steps:
1. **Initialize Time Variables**: 
   - Defines start and end time variables to measure execution duration.

2. **Begin Data Load Process**:
   - Logs the start of data loading for the Silver layer.

3. **Process CRM Tables**:
   - CRM Sales Details:
     - Truncates the `Silver.CRM_sales_details` table.
     - Inserts cleaned and transformed sales data from `Bronze.CRM_sales_details`.
     - Ensures valid date formats and handles missing values using COALESCE.
   - CRM Product Information:
     - Truncates `Silver.CRM_prd_info`.
     - Extracts category and product keys from `prd_key`, cleans product names, and maps product lines.
     - Computes `prd_end_dt` using LEAD function.
   - CRM Customer Information:
     - Truncates `Silver.CRM_cust_info`.
     - Cleans first/last names and standardizes marital status and gender.
     - Uses `ROW_NUMBER()` to ensure only the most recent customer record is kept.

4. **Process ERP Tables**:
   - ERP Product Categories:
     - Truncates `Silver.ERP_px_cat_g1v2`.
     - Cleans category and subcategory data.
   - ERP Locations:
     - Truncates `Silver.ERP_loc_a101`.
     - Removes dashes from `CID` and trims country names.
   - ERP Customer Information:
     - Truncates `Silver.ERP_cust_az12`.
     - Standardizes gender and removes prefixes from `CID`.
     - Ensures birth dates are valid.

5. **Error Handling**:
   - If any error occurs, logs the error message, number, and state.

6. **Execution Time Logging**:
   - Logs the duration of each table's data load.
   - Logs the total execution time for loading the Silver layer.

### Purpose:
This procedure ensures that only clean and transformed data moves from the Bronze layer to the Silver layer,
optimizing the Data Warehouse for further analytics.
*/

CREATE OR ALTER PROC Silver.Load_DataWarehouse AS 
BEGIN
	-- Declare Time Variables
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==============================================';
		PRINT '       >>> Loading Bronze Layer.....';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT '       >>> Loading CRM Tables.....';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.CRM_sales_details"';
		TRUNCATE TABLE DataWarehouse.Silver.CRM_sales_details;

		PRINT 'Loading Table: "Silver.CRM_sales_details"';
		INSERT INTO Silver.CRM_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
			END AS sls_due_dt,
			COALESCE(
					NULLIF(sls_sales, 0), 
					ABS(sls_quantity) * ABS(sls_price)
				) AS sls_sales,

				COALESCE(
					NULLIF(sls_quantity, 0), 
					NULLIF(ABS(sls_sales) / NULLIF(sls_price, 0), 0)
				) AS sls_quantity,

				COALESCE(
					NULLIF(sls_price, 0),
					NULLIF(ABS(sls_sales) / NULLIF(sls_quantity, 0), 0)
				) AS sls_price
		FROM Bronze.CRM_sales_details;
		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.CRM_prd_info"';
		TRUNCATE TABLE DataWarehouse.Silver.CRM_prd_info;

		PRINT 'Loading Table: "Silver.CRM_prd_info"';
		INSERT INTO Silver.CRM_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			SUBSTRING(prd_key, 1,5) AS cat_id,
			SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			DATEADD(day, -1, LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_nm ORDER BY prd_start_dt)) AS prd_end_dt
		FROM Bronze.CRM_prd_info;

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.CRM_cust_info"';
		TRUNCATE TABLE DataWarehouse.Silver.CRM_cust_info;

		PRINT 'Loading Table: "Silver.CRM_cust_info"';
		INSERT INTO Silver.CRM_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE TRIM(UPPER(cst_marital_status))
				WHEN 'S' THEN 'Singel'
				WHEN 'M' THEN 'Married'
				ELSE 'N/A'
			END AS cst_marital_status,
			CASE TRIM(UPPER(cst_gndr))
				WHEN 'F' THEN 'Female'
				WHEN 'M' THEN 'Male'
				ELSE 'N/A'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS flag
			FROM Bronze.CRM_cust_info
			WHERE cst_id IS NOT NULL
		) AS T
		WHERE flag = 1;

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.ERP_px_cat_g1v2"';
		TRUNCATE TABLE DataWarehouse.Silver.ERP_px_cat_g1v2;

		PRINT 'Loading Table: "Silver.ERP_px_cat_g1v2"';
		INSERT INTO Silver.ERP_px_cat_g1v2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT
			TRIM(REPLACE(ID,'_','-')) AS ID,
			TRIM(CAT) AS CAT,
			TRIM(SUBCAT) AS SUBCAT,
			TRIM(MAINTENANCE) AS MAINTENANCE
		FROM Bronze.ERP_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.ERP_loc_a101"';
		TRUNCATE TABLE DataWarehouse.Silver.ERP_loc_a101;

		PRINT 'Loading Table: "Silver.ERP_loc_a101"';
		INSERT INTO Silver.ERP_loc_a101 (
			CID,
			CNTRY
		)
		SELECT
			REPLACE(CID, '-','') AS CID,
			TRIM(CNTRY) AS CNTRY
		FROM Bronze.ERP_loc_a101;

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: "Silver.ERP_cust_az12"';
		TRUNCATE TABLE DataWarehouse.Silver.ERP_cust_az12;

		PRINT 'Loading Table: "Silver.ERP_cust_az12"';
		INSERT INTO Silver.ERP_cust_az12(
			CID,
			BDATE,
			GEN
		)
		SELECT
			CASE
				WHEN UPPER(SUBSTRING(CID,1,3)) = 'NAS' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID,
			CASE
				WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
			CASE
				WHEN TRIM(UPPER(GEN)) IN ('F','FEMALE') THEN 'Female'
				WHEN TRIM(UPPER(GEN)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A'
			END AS GEN
		FROM Bronze.ERP_cust_az12

		SET @end_time = GETDATE();
		PRINT 'Loading Duration: ' + cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' second';
		PRINT '----------------------------------------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer is Completed in: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' second';
		PRINT '==============================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING Silver LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END