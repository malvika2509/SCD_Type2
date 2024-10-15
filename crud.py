from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from .utils import create_target_table_if_not_exists, initial_load_from_source_to_target

def create_record(tablename: str, record: dict, db: Session):
   # Fetch the relevant columns from the source table's metadata
   source_columns = db.execute(text(f"""
       SELECT COLUMN_NAME, COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = :table_name
   """), {'table_name': tablename}).fetchall()
   # Filter out identity columns
   non_identity_columns = [row[0] for row in source_columns if row[1] != 1]
   columns = ', '.join(non_identity_columns)
   values = ', '.join([f':{col}' for col in non_identity_columns])
   query = text(f"""INSERT INTO {tablename} ({columns}) VALUES ({values})""")
   # Add timestamps
   record['UpdatedOn'] = datetime.now()
   db.execute(query, record)
   db.commit()
   return {"message": "Record created successfully"}

def update_record(tablename: str, new_record: dict, db: Session):
   # Fetch the primary key column from SCD_Entities
   primary_key_column = db.execute(text("""
       SELECT Source_Primary_Key_Columns FROM SCD_Entities
       WHERE Source_Table_Name = :tablename
   """), {'tablename': tablename}).fetchone()

   if not primary_key_column:
       raise HTTPException(status_code=404, detail="Primary key column not found")
   
   primary_key_column = primary_key_column[0]

   # Fetch the relevant columns from Table_Columns_Metadata
   metadata_columns = db.execute(text("""
       SELECT Column_Name FROM Table_Columns_Metadata
       WHERE Table_Name = :tablename AND Is_Target_Column = 1
   """), {'tablename': tablename}).fetchall()

   target_columns = [row[0] for row in metadata_columns]

   # Fetch the target table name from SCD_Entities
   target_table_row = db.execute(text("""
       SELECT Target_Table_Name, SCD_Type FROM SCD_Entities
       WHERE Source_Table_Name = :tablename
   """), {'tablename': tablename}).fetchone()

   if not target_table_row:
       raise HTTPException(status_code=404, detail="Target table name not found")
   
   target_table_name, scd_type = target_table_row
   
   # Check and create target table if necessary
   create_target_table_if_not_exists(target_table_name, target_columns, scd_type, db)

   print("target_table_namehduwefw",target_table_name)
   print("target_table_rowfjgrjf",target_table_row)
  
   # Perform initial load from source to target table if target table is empty
   target_table_count = db.execute(text(f"SELECT COUNT(*) FROM {target_table_name}")).scalar()
   print("target_table_count",target_table_count)
   if target_table_count == 0:
       print("Initial load started")
       initial_load_from_source_to_target(tablename, target_table_name, scd_type, db)
   # Fetch the most recent record from the target table
   existing_record = db.execute(text(f"""
       SELECT TOP 1 * FROM {target_table_name} WHERE {primary_key_column} = :{primary_key_column} ORDER BY UpdatedOn DESC
   """), {primary_key_column: new_record[primary_key_column]}).fetchone()

   if scd_type == 'Type 1':
       # SCD Type 1: Overwrite the record
       if existing_record:
           existing_record = dict(existing_record._mapping)
           # Detect changes
           changes_detected = any(
               existing_record[column] != new_record.get(column, existing_record[column])
               for column in target_columns
           )
           
           if changes_detected:
               # Prepare update query
               update_columns = ', '.join([f"{col} = :{col}" for col in target_columns if col not in['StartDate','EndDate','UpdatedOn','IsCurrent']])
               print("update_columns",update_columns)
               db.execute(text(f"""
                   UPDATE {target_table_name}
                   SET {update_columns}, UpdatedOn = :updated_on
                   WHERE {primary_key_column} = :{primary_key_column}
               """), {**new_record, 'updated_on': datetime.now()})
              
               db.commit()
               return {"message": "Record updated successfully (SCD Type 1)"}
           else:
               return {"message": "No changes detected (SCD Type 1)"}
       else:
           # Insert new record directly
           new_row = {col: new_record.get(col, None) for col in target_columns}
           new_row[primary_key_column] = new_record[primary_key_column]
           new_row['UpdatedOn'] = datetime.now()
           columns = ', '.join(new_row.keys())
           values = ', '.join([f":{col}" for col in new_row.keys()])
           db.execute(text(f"""
               INSERT INTO {target_table_name} ({columns}) VALUES ({values})
           """), new_row)
           db.commit()
           return {"message": "Record created successfully (SCD Type 1)"}
   elif scd_type == 'Type 2':
       # SCD Type 2: Handle versioning by closing old records and inserting a new one
       print("whatevrr",target_table_name)
       if existing_record:
           existing_record = dict(existing_record._mapping)
           changes_detected = any(
               existing_record[column] != new_record.get(column, existing_record[column])
               for column in target_columns
           )
           if changes_detected:
               # Update existing record to mark it as inactive
               db.execute(text(f"""
                   UPDATE {target_table_name}
                   SET EndDate = :end_date, UpdatedOn = :updated_on, IsCurrent = 0
                   WHERE {primary_key_column} = :{primary_key_column} AND IsCurrent = 1
               """), {primary_key_column: new_record[primary_key_column], 'end_date': datetime.now(), 'updated_on': datetime.now()})
               # Insert new record
               new_row = {col: new_record.get(col, existing_record[col]) for col in target_columns}
               new_row[primary_key_column] = new_record[primary_key_column]
               new_row['StartDate'] = datetime.now()
               new_row['EndDate'] = None
               new_row['UpdatedOn'] = datetime.now()
               new_row['IsCurrent'] = 1
               columns = ', '.join(new_row.keys())
               values = ', '.join([f":{col}" for col in new_row.keys()])
               print("target_table_namedbfjq3rhfkuqer",target_table_name)

               db.execute(text(f"""
                   INSERT INTO {target_table_name} ({columns})
                   VALUES ({values})
               """), new_row)
               db.commit()
               return {"message": "Record updated successfully (SCD Type 2)"}
           else:
               return {"message": "No changes detected (SCD Type 2)"}
       else:
           # Insert new record directly
           new_row = {col: new_record.get(col, None) for col in target_columns}
           new_row[primary_key_column] = new_record[primary_key_column]
           new_row['StartDate'] = datetime.now()
           new_row['EndDate'] = None
           new_row['UpdatedOn'] = datetime.now()
           new_row['IsCurrent'] = 1
           columns = ', '.join(new_row.keys())
           values = ', '.join([f":{col}" for col in new_row.keys()])
           print("target_table_name",target_table_name)
           db.execute(text(f"""
               INSERT INTO {target_table_name} ({columns}) VALUES ({values})
           """), new_row)
           db.commit()
           return {"message": "Record created successfully (SCD Type 2)"}