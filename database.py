from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
 
DATABASE_URL_1 = 'mssql+pyodbc://@DESKTOP-OP04UAB/Project?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'

 
engine_1 = create_engine(DATABASE_URL_1)

 
SessionLocal_1 = sessionmaker(autocommit=False, autoflush=False, bind=engine_1)

 
def get_db():
    db = SessionLocal_1()
    try:
        yield db
    finally:
        db.close()
 