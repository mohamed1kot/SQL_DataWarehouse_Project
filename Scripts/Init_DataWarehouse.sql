/*
==============================================================================
Create Database and Schemas
==============================================================================
Script Purpose:
	This Script is Check if the Database 'DataWarehouse' is Exist Or not
	if Exit Drop it and re-Create it.
	And Create the Schemas 'Bronze', 'Silver', 'Gold' in The Database.

WARNING:
	if you Runing this Script the Database Will Dropped if it Exists
	and All Data in the database will Deleted 
==============================================================================
*/

USE master;
GO

IF EXISTS(SELECT 1 FROM sys.databases Where name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

--Create DataWarehouse

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas in Database

CREATE SCHEMA Bronze;
GO

CREATE SCHEMA Silver;
GO

CREATE SCHEMA Gold;
GO