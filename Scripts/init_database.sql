/*=============================================================
 🧠  SCRIPT PURPOSE
=============================================================
 This script performs the following tasks:

   1️⃣ Checks if the database 'DataWarehouse' exists.
   2️⃣ If it exists ➜ Drops it (⚠️ all data will be deleted!).
   3️⃣ Creates a fresh 'DataWarehouse' database.
   4️⃣ Inside it, creates three schemas:
         🟤 bronze  → Raw data layer
         ⚪ silver  → Cleaned/transformed data layer
         🟡 gold    → Aggregated/analytics-ready data layer
=============================================================
 ⚠️  WARNING
=============================================================
 Running this script will PERMANENTLY DELETE the existing 
 'DataWarehouse' database (if it exists). 
 Make sure you have backups before executing!
=============================================================*/

-- 🗂️ Step 1: Drop the database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT '⚠️ Dropping existing database: DataWarehouse...';
    DROP DATABASE DataWarehouse;
END
GO

-- 🏗️ Step 2: Create a fresh database
PRINT '🆕 Creating new database: DataWarehouse...';
CREATE DATABASE DataWarehouse;
GO

-- 🧩 Step 3: Switch context to the new database
USE DataWarehouse;
GO

-- 🧱 Step 4: Create schemas (Bronze, Silver, Gold)
PRINT '🏗️ Creating schemas...';
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT '✅ Setup complete! Schemas created successfully.';
