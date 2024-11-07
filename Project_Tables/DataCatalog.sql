USE Project
-- Metadata table for SCD entity configurations
CREATE TABLE SCD_Entities (
    Entity_ID INT IDENTITY(1,1) PRIMARY KEY,
    Entity_Name VARCHAR(50),
    SCD_Type VARCHAR(10),
    Source_Table_Name VARCHAR(50),
    Source_Primary_Key_Columns VARCHAR(100),
    Target_Table_Name VARCHAR(50),
    Target_Surrogate_Key_Column VARCHAR(50),
    Target_Primary_Key_Columns VARCHAR(100),
    Delta_ID_Column VARCHAR(50),
    Overwrite_Flag CHAR(1),
    Created_On DATETIME DEFAULT GETDATE(),
    Updated_On DATETIME DEFAULT GETDATE()
);
--DROP TABLE IF EXISTS SCD_Attributes;

INSERT INTO SCD_Entities (
    Entity_Name,
    SCD_Type,
    Source_Table_Name,
    Source_Primary_Key_Columns,
    Target_Table_Name,
    Target_Surrogate_Key_Column,
    Target_Primary_Key_Columns,
    Delta_ID_Column,
    Overwrite_Flag
)
VALUES 
    (
        'Customer',
        'Type 2',
        'DimCustomer',
        'CustomerID',
        'Dim_Customer',
        'CustomerKey',
        'CustomerID',
        'UpdatedOn',
        'N'
    ),
    (
        'Product',
        'Type 2',
        'DimProduct',
        'ProductID',
        'Dim_Product',
        'ProductKey',
        'ProductID',
        'UpdatedOn',
        'N'
    ),
    (
        'Region',
        'Type 2',
        'DimRegion',
        'RegionID',
        'Dim_Region',
        'RegionKey',
        'RegionID',
        'UpdatedOn',
        'N'
    ),
	(
		'Supplier',
		'Type 2',
		'DimSupplier',
		'SupplierID',
		'Dim_Supplier',
		'SupplierKey',
		'SupplierID',
		'UpdatedOn',
		'N'
    );

CREATE TABLE Table_Columns_Metadata (
    Column_ID INT IDENTITY(1,1) PRIMARY KEY,
    Table_Name VARCHAR(50),
    Column_Name VARCHAR(50),
    Data_Type VARCHAR(20),
    Is_Target_Column BIT,
    Created_On DATETIME DEFAULT DATEADD(day, -1, GETDATE()),
    Updated_On DATETIME DEFAULT DATEADD(day, -1, GETDATE())
);

INSERT INTO Table_Columns_Metadata (
    Table_Name,
    Column_Name,
    Data_Type,
    Is_Target_Column
)
VALUES 
    -- DimCustomer
    ('DimCustomer', 'CustomerKey', 'int', 0),
    ('DimCustomer', 'CustomerID', 'varchar(10)', 1),
    ('DimCustomer', 'Name', 'varchar(50)', 0),
    ('DimCustomer', 'Email', 'varchar(100)', 1),
    ('DimCustomer', 'Address', 'varchar(200)', 1),
    ('DimCustomer', 'City', 'varchar(50)', 1),
    ('DimCustomer', 'State', 'varchar(50)', 1),
    ('DimCustomer', 'Country', 'varchar(50)', 1),
    ('DimCustomer', 'UpdatedOn', 'datetime', 1),

    -- DimProduct
    ('DimProduct', 'ProductKey', 'int', 0),
    ('DimProduct', 'ProductID', 'varchar(10)', 1),
    ('DimProduct', 'ProductName', 'varchar(100)', 1),
    ('DimProduct', 'ProductCategory', 'varchar(50)', 0),
    ('DimProduct', 'Price', 'decimal(10, 2)', 1),
    ('DimProduct', 'UpdatedOn', 'datetime', 1),

    -- DimRegion
    ('DimRegion', 'RegionKey', 'int', 0),
    ('DimRegion', 'RegionID', 'varchar(10)', 1),
    ('DimRegion', 'RegionName', 'varchar(50)', 1),
    ('DimRegion', 'Country', 'varchar(50)', 1),
    ('DimRegion', 'City', 'varchar(50)', 1),
    ('DimRegion', 'State', 'varchar(50)', 1),
    ('DimRegion', 'PostalCode', 'varchar(10)', 0),
    ('DimRegion', 'UpdatedOn', 'datetime', 1),

    -- DimSupplier
    ('DimSupplier', 'SupplierKey', 'int', 0),
	('DimSupplier', 'SupplierID', 'varchar(10)', 1),
    ('DimSupplier', 'SupplierName', 'varchar(100)', 1),
    ('DimSupplier', 'ContactName', 'varchar(100)', 1),
    ('DimSupplier', 'ContactTitle', 'varchar(50)', 0),
    ('DimSupplier', 'Address', 'varchar(255)', 1),
    ('DimSupplier', 'City', 'varchar(50)', 1),
    ('DimSupplier', 'StateProvince', 'varchar(50)', 1),
    ('DimSupplier', 'PostalCode', 'varchar(20)', 0),
    ('DimSupplier', 'Country', 'varchar(50)', 1),
    ('DimSupplier', 'Phone', 'varchar(20)', 1),
    ('DimSupplier', 'Email', 'varchar(100)', 1),
    ('DimSupplier', 'Website', 'varchar(100)', 1),
    ('DimSupplier', 'UpdatedOn', 'datetime', 1);


CREATE TABLE LoadTracking (
    table_name VARCHAR(255),
    last_load_time DATETIME,
	message VARCHAR(255)
);
------------------------------------------------------------------------------------------------------------

-- METADATA TABLES

SELECT * FROM Table_Columns_Metadata
--change postal code as relevant column
--UPDATE Table_Columns_Metadata SET Is_Target_Column=1,Updated_On=GETDATE()  WHERE Column_ID=32
SELECT * FROM SCD_Entities;
SELECT * FROM LoadTracking

-- SCD Type 2  Target table
SELECT * FROM Dim_Customer

-- SCD Type 1 Target table
SELECT * FROM Dim_Product

-- Dynamic column insertion 
SELECT * FROM Dim_Supplier

---for OVERWRITE

INSERT INTO Dim_Region (RegionID, RegionName, Country, City, State, UpdatedOn)
VALUES 
    ('R001', 'New York Metro', 'USA', 'New York', 'NY', '2023-01-01'),
    ('R002', 'Los Angeles Metro', 'USA', 'Los Angeles', '90001', '2023-01-01'),
    ('R003', 'Chicago Metro', 'USA', 'Chicago', 'IL', '2023-01-01'),
    ('R004', 'Houston Metro', 'USA', 'Houston', 'TX', '2023-01-01');

SELECT * FROM Dim_Region
--truncate table Dim_Region

---- drops and truncates

--truncate table Dim_Customer
--TRUNCATE TABLE LoadTracking

--DROP TABLE Dim_Product
--DROP TABLE Dim_Customer
--DROP TABLE Dim_Supplier


--DROP TABLE Table_Columns_Metadata
--DROP TABLE SCD_Entities