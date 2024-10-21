from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from database import get_db

# Helper Functions
def fetch_metadata_columns(tablename: str, db: Session ):
    return db.execute(text("""
        SELECT Column_Name FROM Table_Columns_Metadata
        WHERE Table_Name = :tablename AND Is_Target_Column = 1
    """), {'tablename': tablename}).fetchall()

def fetch_entity_info(tablename: str, db: Session ):
    return db.execute(text("""
        SELECT Source_Primary_Key_Columns, Overwrite_Flag FROM SCD_Entities
        WHERE Source_Table_Name = :tablename
    """), {'tablename': tablename}).fetchone()

def fetch_target_table_info(tablename: str, db: Session ):
    return db.execute(text("""
        SELECT Target_Table_Name, SCD_Type FROM SCD_Entities
        WHERE Source_Table_Name = :tablename
    """), {'tablename': tablename}).fetchone()

def fetch_last_load_time(target_table_name: str, db: Session ):
    return db.execute(text("""
        SELECT last_load_time FROM LoadTracking
        WHERE table_name = :table_name
    """), {'table_name': target_table_name}).fetchone()

def fetch_new_records(tablename: str, target_table_name: str,primary_key_column: str,last_load_time: datetime, db: Session ):
    print(tablename,last_load_time,target_table_name,primary_key_column)
    records = db.execute(text(f"""
        SELECT src.*
        FROM {tablename} src
        LEFT Outer JOIN {target_table_name} tgt
        ON src.{primary_key_column} = tgt.{primary_key_column}
        WHERE src.UpdatedOn > :last_load_time  
    """), {'last_load_time': last_load_time}).fetchall()
    
    return records

def fetch_new_records_count(tablename: str, last_load_time: datetime, db: Session ):
    return db.execute(text(f"""
        SELECT COUNT(*) FROM {tablename}
        WHERE UpdatedOn > :last_load_time
    """), {'last_load_time': last_load_time}).scalar()

def fetch_existing_record(target_table_name: str, primary_key_column: str, primary_key_value: str, db: Session ):
    return db.execute(text(f"""
        SELECT TOP 1 * FROM {target_table_name} WHERE {primary_key_column} = :{primary_key_column} ORDER BY UpdatedOn DESC
    """), {primary_key_column: primary_key_value}).fetchone()