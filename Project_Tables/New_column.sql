USE Project;
GO

-- Create the table with the new SupplierID column
CREATE TABLE DimSupplier (
    SupplierKey INT PRIMARY KEY,
    SupplierID VARCHAR(10) NOT NULL,
    SupplierName VARCHAR(100) NOT NULL,
    ContactName VARCHAR(100),
    ContactTitle VARCHAR(50),
    Address VARCHAR(255),
    City VARCHAR(50),
    StateProvince VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(20),
    Email VARCHAR(100),
    Website VARCHAR(100),
    UpdatedOn DATETIME DEFAULT GETDATE()
);

-- Insert data into the table
INSERT INTO DimSupplier (SupplierKey, SupplierID, SupplierName, ContactName, ContactTitle, Address, City, StateProvince, PostalCode, Country, Phone, Email, Website) VALUES
(1, 'SUP001', 'Alpha Supplies', 'John Doe', 'Manager', '123 Elm St', 'Springfield', 'IL', '62701', 'USA', '555-1234', 'john.doe@alphasupplies.com', 'www.alphasupplies.com'),
(2, 'SUP002', 'Beta Traders', 'Jane Smith', 'Sales Rep', '456 Oak St', 'Shelbyville', 'IL', '62565', 'USA', '555-5678', 'jane.smith@betatraders.com', 'www.betatraders.com'),
(3, 'SUP003', 'Gamma Goods', 'Alice Johnson', 'Director', '789 Pine St', 'Capital City', 'IL', '62702', 'USA', '555-8765', 'alice.johnson@gammagoods.com', 'www.gammagoods.com'),
(4, 'SUP004', 'Delta Distributors', 'Bob Brown', 'CEO', '101 Maple St', 'Ogdenville', 'IL', '62563', 'USA', '555-4321', 'bob.brown@deltadistributors.com', 'www.deltadistributors.com'),
(5, 'SUP005', 'Epsilon Enterprises', 'Charlie Davis', 'CFO', '202 Birch St', 'North Haverbrook', 'IL', '62564', 'USA', '555-6789', 'charlie.davis@epsilonenterprises.com', 'www.epsilonenterprises.com'),
-- Add more records as needed
(6, 'SUP006', 'Zeta Supplies', 'David Wilson', 'Manager', '303 Cedar St', 'Springfield', 'IL', '62701', 'USA', '555-2345', 'david.wilson@zetasupplies.com', 'www.zetasupplies.com'),
(7, 'SUP007', 'Eta Traders', 'Eve White', 'Sales Rep', '404 Walnut St', 'Shelbyville', 'IL', '62565', 'USA', '555-3456', 'eve.white@etatraders.com', 'www.etatraders.com'),
(8, 'SUP008', 'Theta Goods', 'Frank Green', 'Director', '505 Ash St', 'Capital City', 'IL', '62702', 'USA', '555-4567', 'frank.green@thetagoods.com', 'www.thetagoods.com'),
(9, 'SUP009', 'Iota Distributors', 'Grace Black', 'CEO', '606 Poplar St', 'Ogdenville', 'IL', '62563', 'USA', '555-5678', 'grace.black@iotadistributors.com', 'www.iotadistributors.com'),
(10, 'SUP010', 'Kappa Enterprises', 'Hank Blue', 'CFO', '707 Fir St', 'North Haverbrook', 'IL', '62564', 'USA', '555-6789', 'hank.blue@kappaenterprises.com', 'www.kappaenterprises.com'),
-- Continue adding records up to 50
(11, 'SUP011', 'Lambda Supplies', 'Ivy Brown', 'Manager', '808 Elm St', 'Springfield', 'IL', '62701', 'USA', '555-7890', 'ivy.brown@lambdasupplies.com', 'www.lambdasupplies.com'),
(12, 'SUP012', 'Mu Traders', 'Jack White', 'Sales Rep', '909 Oak St', 'Shelbyville', 'IL', '62565', 'USA', '555-8901', 'jack.white@mutraders.com', 'www.mutraders.com'),
(13, 'SUP013', 'Nu Goods', 'Karen Green', 'Director', '1010 Pine St', 'Capital City', 'IL', '62702', 'USA', '555-9012', 'karen.green@nugoods.com', 'www.nugoods.com'),
(14, 'SUP014', 'Xi Distributors', 'Larry Black', 'CEO', '1111 Maple St', 'Ogdenville', 'IL', '62563', 'USA', '555-0123', 'larry.black@xidistributors.com', 'www.xidistributors.com'),
(15, 'SUP015', 'Omicron Enterprises', 'Mona Blue', 'CFO', '1212 Birch St', 'North Haverbrook', 'IL', '62564', 'USA', '555-1234', 'mona.blue@omicronenterprises.com', 'www.omicronenterprises.com'),
(16, 'SUP016', 'Pi Supplies', 'Nina Brown', 'Manager', '1313 Cedar St', 'Springfield', 'IL', '62701', 'USA', '555-2345', 'nina.brown@pisupplies.com', 'www.pisupplies.com'),
(17, 'SUP017', 'Rho Traders', 'Oscar White', 'Sales Rep', '1414 Walnut St', 'Shelbyville', 'IL', '62565', 'USA', '555-3456', 'oscar.white@rhotraders.com', 'www.rhotraders.com'),
(18, 'SUP018', 'Sigma Goods', 'Paul Green', 'Director', '1515 Ash St', 'Capital City', 'IL', '62702', 'USA', '555-4567', 'paul.green@sigmagoods.com', 'www.sigmagoods.com'),
(19, 'SUP019', 'Tau Distributors', 'Quinn Black', 'CEO', '1616 Poplar St', 'Ogdenville', 'IL', '62563', 'USA', '555-5678', 'quinn.black@taudistributors.com', 'www.taudistributors.com'),
(20, 'SUP020', 'Upsilon Enterprises', 'Rita Blue', 'CFO', '1717 Fir St', 'North Haverbrook', 'IL', '62564', 'USA', '555-6789', 'rita.blue@upsilonenterprises.com', 'www.upsilonenterprises.com'),
(21, 'SUP021', 'Phi Supplies', 'Sam Brown', 'Manager', '1818 Elm St', 'Springfield', 'IL', '62701', 'USA', '555-7890', 'sam.brown@phisupplies.com', 'www.phisupplies.com'),
(22, 'SUP022', 'Chi Traders', 'Tina White', 'Sales Rep', '1919 Oak St', 'Shelbyville', 'IL', '62565', 'USA', '555-8901', 'tina.white@chitraders.com', 'www.chitraders.com'),
(23, 'SUP023', 'Psi Goods', 'Uma Green', 'Director', '2020 Pine St', 'Capital City', 'IL', '62702', 'USA', '555-9012', 'uma.green@psigoods.com', 'www.psigoods.com'),
(24, 'SUP024', 'Omega Distributors', 'Vic Black', 'CEO', '2121 Maple St', 'Ogdenville', 'IL', '62563', 'USA', '555-0123', 'vic.black@omegadistributors.com', 'www.omegadistributors.com'),
(25, 'SUP025', 'Alpha Beta Enterprises', 'Wendy Blue', 'CFO', '2222 Birch St', 'North Haverbrook', 'IL', '62564', 'USA', '555-1234', 'wendy.blue@alphabetaenterprises.com', 'www.alphabetaenterprises.com'),
(26, 'SUP026', 'Beta Gamma Supplies', 'Xander Brown', 'Manager', '2323 Cedar St', 'Springfield', 'IL', '62701', 'USA', '555-2345', 'xander.brown@betagammasupplies.com', 'www.betagammasupplies.com'),
(27, 'SUP027', 'Gamma Delta Traders', 'Yara White', 'Sales Rep', '2424 Walnut St', 'Shelbyville', 'IL', '62565', 'USA', '555-3456', 'yara.white@gammadeltatraders.com', 'www.gammadeltatraders.com'),
(28, 'SUP028', 'Delta Epsilon Goods', 'Zane Green', 'Director', '2525 Ash St', 'Capital City', 'IL', '62702', 'USA', '555-4567', 'zane.green@deltaepsilongoods.com', 'www.deltaepsilongoods.com');
SELECT * FROM DimSupplier

--ALTER TABLE DimSupplier DROP COLUMN FaxNumber
