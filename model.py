from sqlalchemy import Column, Integer, String, DECIMAL, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DimCustomer(Base):
    __tablename__ = 'DimCustomer'
    CustomerKey = Column(Integer, primary_key=True, index=True)
    CustomerID = Column(String(10), unique=True, index=True)
    Name = Column(String(50))
    Email = Column(String(100))
    Address = Column(String(200))
    City = Column(String(50))
    State = Column(String(50))
    Country = Column(String(50))
    UpdatedOn = Column(DateTime)

class DimProduct(Base):
    __tablename__ = 'DimProduct'
    ProductKey = Column(Integer, primary_key=True, index=True)
    ProductID = Column(String(10), unique=True, index=True)
    ProductName = Column(String(100))
    ProductCategory = Column(String(50))
    Price = Column(DECIMAL(10, 2))
    UpdatedOn = Column(DateTime)

class DimRegion(Base):
    __tablename__ = 'DimRegion'
    RegionKey = Column(Integer, primary_key=True, index=True)
    RegionID = Column(String(10), unique=True, index=True)
    RegionName = Column(String(50))
    Country = Column(String(50))
    City = Column(String(50))
    State = Column(String(50))
    PostalCode = Column(String(10))
    UpdatedOn = Column(DateTime)
