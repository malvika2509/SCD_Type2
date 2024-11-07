USE Project
GO

-- Create dimension tables
CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(10),
    Name VARCHAR(50),
    Email VARCHAR(100),
    Address VARCHAR(200),
    City VARCHAR(50),
    State VARCHAR(50),
    Country VARCHAR(50),
    UpdatedOn DATETIME
);

CREATE TABLE DimProduct (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID VARCHAR(10),
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    Price DECIMAL(10, 2),
    UpdatedOn DATETIME
);

CREATE TABLE DimRegion (
    RegionKey INT IDENTITY(1,1) PRIMARY KEY,
    RegionID VARCHAR(10),
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(50),
    PostalCode VARCHAR(10),
    UpdatedOn DATETIME
);

-- Create fact table
CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey INT,
    ProductKey INT,
    RegionKey INT,
    SalesAmount DECIMAL(10, 2),
    Quantity INT,
    CONSTRAINT FK_CustomerKey FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    CONSTRAINT FK_ProductKey FOREIGN KEY (ProductKey) REFERENCES DimProduct(ProductKey),
    CONSTRAINT FK_RegionKey FOREIGN KEY (RegionKey) REFERENCES DimRegion(RegionKey)
);

-- Insert data into dimension tables
INSERT INTO DimCustomer (CustomerID, Name, Email, Address, City, State, Country, UpdatedOn)
VALUES 
    ('C001', 'John Doe', 'johndoe@example.com', '123 Main St', 'New York', 'NY', 'USA', '2023-01-01'),
    ('C002', 'Jane Doe', 'janedoe@example.com', '456 Elm St', 'Los Angeles', 'CA', 'USA', '2023-01-01'),
    ('C003', 'Bob Smith', 'bobsmith@example.com', '789 Oak St', 'Chicago', 'IL', 'USA', '2023-01-01'),
    ('C004', 'Alice Johnson', 'alicejohnson@example.com', '321 Maple St', 'Houston', 'TX', 'USA', '2023-01-01'),
    ('C005', 'Mike Davis', 'mikedavis@example.com', '901 Broadway', 'Seattle', 'WA', 'USA', '2023-01-01'),
    ('C006', 'Emily Chen', 'emilychen@example.com', '456 5th Ave', 'Miami', 'FL', 'USA', '2023-01-01'),
    ('C007', 'David Lee', 'davidlee@example.com', '123 Wall St', 'Boston', 'MA', 'USA', '2023-01-01'),
    ('C008', 'Sarah Taylor', 'sarahtaylor@example.com', '789 Park Ave', 'Denver', 'CO', 'USA', '2023-01-01'),
    ('C009', 'Kevin White', 'kevinwhite@example.com', '456 6th St', 'Phoenix', 'AZ', 'USA', '2023-01-01'),
    ('C010', 'Lisa Nguyen', 'lisanguyen@example.com', '901 Main St', 'Dallas', 'TX', 'USA', '2023-01-01');

INSERT INTO DimProduct (ProductID, ProductName, ProductCategory, Price, UpdatedOn)
VALUES 
    ('P001', 'iPhone', 'Electronics', 999.99, '2023-01-01'),
    ('P002', 'Samsung TV', 'Electronics', 1299.99, '2023-01-01'),
    ('P003', 'Apple Watch', 'Electronics', 399.99, '2023-01-01'),
    ('P004', 'Sony Headphones', 'Electronics', 99.99, '2023-01-01'),
    ('P005', 'Google Pixel', 'Electronics', 799.99,  '2023-01-01'),
    ('P006', 'LG Laptop', 'Electronics', 1299.99,  '2023-01-01'),
    ('P007', 'Amazon Echo', 'Electronics', 49.99,  '2023-01-01'),
    ('P008', 'Microsoft Surface', 'Electronics', 1999.99,  '2023-01-01'),
    ('P009', 'Canon Camera', 'Electronics', 499.99,  '2023-01-01'),
    ('P010', 'HP Printer', 'Electronics', 99.99,  '2023-01-01');

INSERT INTO DimRegion (RegionID, RegionName, Country, City, State, PostalCode, UpdatedOn)
VALUES 
    ('R001', 'New York Metro', 'USA', 'New York', 'NY', '10001', '2023-01-01'),
    ('R002', 'Los Angeles Metro', 'USA', 'Los Angeles', 'CA', '90001', '2023-01-01'),
    ('R003', 'Chicago Metro', 'USA', 'Chicago', 'IL', '60601', '2023-01-01'),
    ('R004', 'Houston Metro', 'USA', 'Houston', 'TX', '77001', '2023-01-01'),
    ('R005', 'Seattle Metro', 'USA', 'Seattle', 'WA', '98101',  '2023-01-01'),
    ('R006', 'Miami Metro', 'USA', 'Miami', 'FL', '33101',  '2023-01-01'),
    ('R007', 'Boston Metro', 'USA', 'Boston', 'MA', '02101',  '2023-01-01'),
    ('R008', 'Denver Metro', 'USA', 'Denver', 'CO', '80201',  '2023-01-01'),
    ('R009', 'Phoenix Metro', 'USA', 'Phoenix', 'AZ', '85001',  '2023-01-01'),
    ('R010', 'Dallas Metro', 'USA', 'Dallas', 'TX', '75201',  '2023-01-01');

INSERT INTO FactSales (CustomerKey, ProductKey, RegionKey, SalesAmount, Quantity)
VALUES 
    (1, 1, 1, 999.99, 1),
    (1, 2, 2, 1299.99, 1),
    (2, 1, 1, 999.99, 1),
    (3, 1, 3, 999.99, 1),
    (4, 2, 4, 1299.99, 1),
    (5, 3, 5, 399.99, 1),
    (6, 4, 6, 99.99, 1),
    (7, 5, 7, 799.99, 1),
    (8, 6, 8, 1299.99, 1),
    (9, 7, 9, 49.99, 1),
    (10, 8, 10, 1999.99, 1),
    (1, 9, 1, 499.99, 1),
    (2, 10, 2, 99.99, 1),
    (3, 1, 3, 999.99, 1),
    (4, 2, 4, 1299.99, 1),
    (5, 3, 5, 399.99, 1),
    (6, 4, 6, 99.99, 1),
    (7, 5, 7, 799.99, 1),
    (8, 6, 8, 1299.99, 1),
    (9, 7, 9, 49.99, 1),
    (10, 8, 10, 1999.99, 1);
-----------------------------------------------------------------------------------------------

-- Select data from tables
SELECT * FROM DimProduct;
SELECT * FROM DimCustomer;
SELECT * FROM DimRegion;
SELECT * FROM FactSales;



TRUNCATE TABLE DimCustomer
TRUNCATE TABLE DimProduct
TRUNCATE TABLE DimRegion

--DROP TABLE DimCustomer
--DROP TABLE DimProduct
--DROP TABLE DimRegion
--DROP TABLE FactSales



