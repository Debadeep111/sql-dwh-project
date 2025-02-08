IF OBJECT_ID ('bronze.crm_cust_info','u') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
  cst_id int,
  cst_key nvarchar(50),
  cst_first_name nvarchar(50),
  cst_last_name nvarchar(50),
  cst_marital_status nvarchar(50),
  cst_gender nvarchar(50),
  cst_create_date date);
if OBJECT_ID ('bronze.crm_prd_info','u') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
  prd_id int,
  prd_key nvarchar(50),
  prd_nm nvarchar(50),
  prd_cost	nvarchar(50),
  prd_line	nvarchar(50),
  prd_start_dt date,
  prd_end_dt date);
if OBJECT_ID ('bronze.crm_sales_details','u') IS NOT NULL
DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     DECIMAL(10,2),
    sls_quantity  INT,
    sls_price     DECIMAL(10,2));
if OBJECT_ID ('bronze.erp_cust_az12','u') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    CID    NVARCHAR(50),
    BDATE  DATE,
    GEN    NVARCHAR(10)
);
if OBJECT_ID ('bronze.erp_loc_a101','u') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    CID   NVARCHAR(50),
    CNTRY NVARCHAR(50)
);
if OBJECT_ID ('bronze.px_cat_g1v2','u') IS NOT NULL
DROP TABLE bronze.px_cat_g1v2;
CREATE TABLE bronze.px_cat_g1v2 (
    ID          NVARCHAR(50),
    CAT         NVARCHAR(50),
    SUBCAT      NVARCHAR(50),
    MAINTENANCE NVARCHAR(10)
);


create or alter procedure bronze.load_bronze as begin
declare @start_time datetime,@end_time datetime, @start_bronze datetime,@end_bronze datetime
	set @start_bronze=GETDATE();
	set @start_time=GETDATE();
	print 'LOADING BRONZE LAYER';
	Print 'LOADING FROM CRM';
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	set @start_time=GETDATE();
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
		firstrow=2,
		fieldterminator=',',
		tablock
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	set @start_time=GETDATE();
	TRUNCATE TABLE bronze.crm_sales_details;

	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	
	PRINT 'LOADING FROM ERP';
	set @start_time=GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;

	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	set @start_time=GETDATE();
	TRUNCATE TABLE bronze.erp_cust_az12;

	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	set @start_time=GETDATE();
	TRUNCATE TABLE bronze.erp_loc_a101;

	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\hp\OneDrive\Desktop\DWH Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	set @end_time=GETDATE();
	print 'loading time :'+cast(datediff(second,@start_time,@end_time) as nvarchar)
	set @end_bronze=GETDATE()
	print 'LOADING TIME OF BRONZE LAYER: '+ cast(datediff(second,@start_bronze,@end_bronze) as nvarchar)
end


exec bronze.load_bronze


