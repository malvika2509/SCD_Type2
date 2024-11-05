from time import sleep
from faker import Faker
from datetime import datetime
from database import SessionLocal_1
from model import DimCustomer, DimProduct, DimRegion

fake = Faker()

# Generate fake data
def generate_data():
    customer_data = {
        "CustomerID": fake.unique.random_number(digits=5, fix_len=True),
        "Name": fake.name(),
        "Email": fake.email(),
        "Address": fake.address()[:50],
        "City": fake.city()[:50],
        "State": fake.state()[:50],
        "Country": fake.country()[:50],
        "UpdatedOn": datetime.now()
    }
    product_data = {
        "ProductID": fake.unique.random_number(digits=5, fix_len=True),
        "ProductName": fake.word(),
        "ProductCategory": fake.word(),
        "Price": round(fake.random_number(digits=2), 2),
        "UpdatedOn": datetime.now()
    }
    region_data = {
        "RegionID": fake.unique.random_number(digits=5, fix_len=True),
        "RegionName": fake.city()[:50],
        "Country": fake.country()[:50],
        "City": fake.city()[:50],
        "State": fake.state()[:50],
        "PostalCode": fake.postcode(),
        "UpdatedOn": datetime.now()
    }
    return customer_data, product_data, region_data

# Insert data into the database
def insert_data(session, data, table):
    if table == 'customer_data':
        new_customer = DimCustomer(**data)
        session.add(new_customer)
    elif table == 'product_data':
        new_product = DimProduct(**data)
        session.add(new_product)
    elif table == 'region_data':
        new_region = DimRegion(**data)
        session.add(new_region)
    session.commit()

# Produce data asynchronously
def produce_data():
    while True:
        print("syncing faker data")
        
        customer_data, product_data, region_data = generate_data()
        # Use synchronous session in an asynchronous function
        with SessionLocal_1() as session:
            insert_data(session, customer_data, 'customer_data')
            insert_data(session, product_data, 'product_data')
            insert_data(session, region_data, 'region_data')
        sleep(120)  # Wait 120 seconds before producing new data
