from datetime import datetime, timedelta
from fastapi import Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from database import get_db
from logging_anurag import log_update

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
    
def add_new_column_if_needed(db: Session, source_table_name: str):
    print("CHECKIN FOR NEW COLUMN")
    # Fetch the target table name from the metadata
    target_table_name_result = db.execute(text("""
        SELECT Target_Table_Name
        FROM SCD_Entities
        WHERE Source_Table_Name = :source_table_name
    """), {'source_table_name': source_table_name}).fetchone()
    # print("TARGET TABLE NAME-new column function",target_table_name_result)

    if not target_table_name_result:
        raise HTTPException(status_code=404, detail=f"Target table name not found for source table '{source_table_name}'")

    target_table_name = target_table_name_result[0]
    print("TARGET TABLE NAME-new column function",target_table_name)

    # Fetch columns where Is_Target_Column has been changed to 1
    new_columns = db.execute(text("""
        SELECT Column_Name, Data_Type
        FROM Table_Columns_Metadata
        WHERE Is_Target_Column = 1 AND Updated_On > Created_On AND Table_Name = :source_table_name
    """), {'source_table_name': source_table_name}).fetchall()
    print("NEW COLUMNS--new column function",new_columns)

    for column in new_columns:
        column_name, data_type = column
        print("NEW COLUMN-new column function",column)
        # Check if the column already exists in the target table
        existing_columns = db.execute(text(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :target_table_name AND COLUMN_NAME = :column_name
        """), {'target_table_name': target_table_name, 'column_name': column_name}).fetchall()

        print("EXISTING COLUMN-------", existing_columns)

        if not existing_columns:
            # Add the new column to the target table
            alter_table_query = f"""
            ALTER TABLE {target_table_name}
            ADD {column_name} {data_type} NULL
            """
            log_update(target_table_name,"Type 2","dynamic column added")
            db.execute(text(alter_table_query))
            db.commit()
            print(f"Column '{column_name}' added to table '{target_table_name}' successfully.")