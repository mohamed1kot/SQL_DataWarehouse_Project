/*
==============================================================================
DDL Scripts: Creating Tables In a Silver Layer
==============================================================================

Script:
	This Script is Used To Create a Tables in a Silver Schema and Droping tables
	if already Exists and redifine The all Tables again.
==============================================================================

*/


USE DataWarehouse;
GO

PRINT '==================================================='
PRINT '    >>>>Creating Tables In a Silver Layer'
PRINT '==================================================='


IF OBJECT_ID('Silver.CRM_Cust_info', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_Cust_info;
GO

CREATE TABLE Silver.CRM_Cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	DW_Date_Loading DATETIME DEFAULT GETDATE()
) ON [PRIMARY];
GO


IF OBJECT_ID('Silver.CRM_prd_info', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_prd_info;
GO

CREATE TABLE Silver.CRM_prd_info
(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	DW_Date_Loading DATETIME DEFAULT GETDATE()
) ON [PRIMARY];
GO


IF OBJECT_ID('Silver.CRM_sales_details', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_sales_details;
GO

CREATE TABLE Silver.CRM_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales FLOAT,
	sls_quantity INT,
	sls_price FLOAT,
	DW_Date_Loading DATETIME DEFAULT GETDATE()
) ON [PRIMARY];
GO

IF OBJECT_ID('Silver.ERP_cust_az12', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_cust_az12;
GO

CREATE TABLE Silver.ERP_cust_az12
(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50),
	DW_Date_Loading DATETIME DEFAULT GETDATE()

) ON [PRIMARY];
GO

IF OBJECT_ID('Silver.ERP_loc_a101', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_loc_a101;
GO

CREATE TABLE Silver.ERP_loc_a101
(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	DW_Date_Loading DATETIME DEFAULT GETDATE()
) ON [PRIMARY];
GO

IF OBJECT_ID('Silver.ERP_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_px_cat_g1v2;
GO

CREATE TABLE Silver.ERP_px_cat_g1v2
(

	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	DW_Date_Loading DATETIME DEFAULT GETDATE()
) ON [PRIMARY];
GO