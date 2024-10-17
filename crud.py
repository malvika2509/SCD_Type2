from datetime import datetime
from fastapi import Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from .helper import fetch_entity_info,fetch_existing_record,fetch_metadata_columns,fetch_last_load_time,fetch_new_records,fetch_new_records_count,fetch_target_table_info
from .database import get_db
from .utils import create_target_table_if_not_exists, initial_load_from_source_to_target
from .logger import logger
from .logging import log_fail, log_incrementalloading, log_update, log_overwrite

def create_record(tablename: str, record: dict, db: Session = Depends(get_db)):
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


def update_record(tablename: str, new_record: dict, db: Session = Depends(get_db)):
    
    # Fetch the primary key column and overwrite flag from SCD_Entities
    entity_info = fetch_entity_info(tablename, db)
    
    if not entity_info:
        log_fail('update_record', 'Entity information not found ', tablename)
        raise HTTPException(status_code=404, detail="Entity information not found")
    
    primary_key_column, overwrite_flag = entity_info
    print("entity info",entity_info)

    # Fetch the relevant columns from Table_Columns_Metadata
    metadata_columns = fetch_metadata_columns(tablename, db)
    
    target_columns = [row[0] for row in metadata_columns]

    # Fetch the target table name from SCD_Entities
    target_table_row = fetch_target_table_info(tablename, db)

    if not target_table_row:
        log_fail('update_record', 'Target table name not found', tablename)
        raise HTTPException(status_code=404, detail="Target table name not found")

    target_table_name, scd_type = target_table_row

    # Check and create target table if necessary
    create_target_table_if_not_exists(target_table_name, target_columns, scd_type, db)

    print("target_table_namehduwefw",target_table_name)
    print("target_table_rowfjgrjf",target_table_row)
    
    # If Overwrite_Flag is 'Y', truncate the target table and reload from the source table
    if overwrite_flag == 'Y':
        db.execute(text(f"TRUNCATE TABLE {target_table_name}"))
        initial_load_from_source_to_target(tablename, target_table_name, scd_type, db)
        db.execute(text("""
            INSERT INTO LoadTracking (table_name, last_load_time, message)
            VALUES (:table_name, :last_load_time, :message)
        """), {'table_name': target_table_name, 'last_load_time': datetime.now(), 'message': "overwrite flag set, table reloaded"})
        log_overwrite(tablename)
        db.commit()
        return {"message": "Target table reloaded successfully due to overwrite flag"}

    # Perform initial load from source to target table if target table is empty
    target_table_count = db.execute(text(f"SELECT COUNT(*) FROM {target_table_name}")).scalar()
    print("target_table_count",target_table_count)
    
    if target_table_count == 0:
        print("Initial load started")
        initial_load_from_source_to_target(tablename, target_table_name, scd_type, db)
        # Record the initial load time
        db.execute(text("""
            INSERT INTO LoadTracking (table_name, last_load_time,message)
            VALUES (:table_name, :last_load_time,:message)
        """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"initial load"})
        
        db.commit()
        
    # Fetch the last load time from LoadTracking
    last_load_time = fetch_last_load_time(target_table_name, db)

    if not last_load_time:
        raise HTTPException(status_code=404, detail="Last load time not found")
    
    print("last_load_time",last_load_time)
    
    last_load_time = last_load_time[0]
    
    # Fetch new or updated records from the source table since the last load time
    new_records = fetch_new_records(tablename, last_load_time, db)
    new_records_count = fetch_new_records_count(tablename, last_load_time, db)
    # Get the count of new or updated records
    
    print("new_records_count",new_records_count)
    
    log_incrementalloading(tablename,new_records_count)
    print("new records")
        
    # Insert new records into the target table
    for record in new_records:
        record = dict(record._mapping)
        # print("record",record)
        new_row = {col: record.get(col, None) for col in target_columns if col not in['StartDate','EndDate','UpdatedOn','IsCurrent']}
        
        new_row[primary_key_column] = record[primary_key_column]
        new_row['UpdatedOn'] = datetime.now()
        
        # print("new_row",new_row)
        
        if scd_type == 'Type 2':
            new_row['StartDate'] = datetime.now()
            new_row['EndDate'] = None
            new_row['IsCurrent'] = 1
        
        columns = ', '.join(new_row.keys())
        values = ', '.join([f":{col}" for col in new_row.keys()])
        db.execute(text(f"""
            INSERT INTO {target_table_name} ({columns}) VALUES ({values})
        """), new_row)
        
        # print("inserted")
        
        db.commit()
    db.execute(text("""
            INSERT INTO LoadTracking (table_name, last_load_time,message)
            VALUES (:table_name, :last_load_time,:message)
        """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"compared to source,data inserted"})
        
    db.commit()
        
    # Fetch the most recent record from the target table
    try:
        existing_record = fetch_existing_record(target_table_name, primary_key_column, new_record[primary_key_column], db)
    except KeyError as e:
        log_fail('update_record', f'Missing primary key column: {str(e)}', tablename)
        raise HTTPException(status_code=400, detail=f"Missing primary key column: {str(e)}")
    
    print("existing_record",existing_record)

    if scd_type == 'Type 1':
        # SCD Type 1: Overwrite the record
        if existing_record:     
            existing_record = dict(existing_record._mapping)
            print("existing_record_tye1",existing_record)
            print("new_record_type1",new_record)
            # Detect changes
            # Detect changes
            filtered_new_record = {col: new_record[col] for col in target_columns if col in new_record}
            print("filtered_new_record",filtered_new_record)
            changes_detected = any(
                existing_record[column]!= filtered_new_record.get(column, existing_record[column])
                for column in target_columns if column not in ['StartDate', 'EndDate', 'UpdatedOn', 'IsCurrent']
            )

            print("changes_detected", changes_detected)
            if changes_detected:
                # Prepare update query
                update_columns = ', '.join([f"{col} = :{col}" for col in target_columns if col not in['StartDate','EndDate','UpdatedOn','IsCurrent']])
                print("update_columns",update_columns)
                
                db.execute(text(f"""
                    UPDATE {target_table_name}
                    SET {update_columns}, UpdatedOn = :updated_on
                    WHERE {primary_key_column} = :{primary_key_column}
                """), {**new_record, 'updated_on': datetime.now()})
                
                db.execute(text("""
                    INSERT INTO LoadTracking (table_name, last_load_time,message)
                    VALUES (:table_name, :last_load_time,:message)
                """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"request received,change occurred in existing record"})
                
                db.commit()
                log_update(tablename,scd_type)
                return {"message": "Record updated successfully (SCD Type 1)"}
            else:
                return {"message": "No changes detected (SCD Type 1)"}
        else:
            # Insert new record directly
            new_row = {col: new_record.get(col, None) for col in target_columns if col not in['StartDate','EndDate','UpdatedOn','IsCurrent']}
            print("new_row",new_row)
            new_row[primary_key_column] = new_record[primary_key_column]
            new_row['UpdatedOn'] = datetime.now()
            columns = ', '.join(new_row.keys())
            values = ', '.join([f":{col}" for col in new_row.keys()])
            db.execute(text(f"""
                INSERT INTO {target_table_name} ({columns}) VALUES ({values})
            """), new_row)
            db.execute(text("""
                    INSERT INTO LoadTracking (table_name, last_load_time,message)
                    VALUES (:table_name, :last_load_time,:message)
                """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"new record added"})
            
            db.commit()
            log_update(tablename,scd_type)
            return {"message": "Record created successfully (SCD Type 1)"}
    elif scd_type == 'Type 2':
        # SCD Type 2: Handle versioning by closing old records and inserting a new one
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
                
                db.execute(text("""
                    INSERT INTO LoadTracking (table_name, last_load_time,message)
                    VALUES (:table_name, :last_load_time,:message)
                """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"made record inactive"})
                
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
                
                db.execute(text("""
                    INSERT INTO LoadTracking (table_name, last_load_time,message)
                    VALUES (:table_name, :last_load_time,:message)
                """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"inserted new changes(SCD 2)"})
                
                db.commit()
                log_update(tablename,scd_type)
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
            
            db.execute(text("""
                        INSERT INTO LoadTracking (table_name, last_load_time,message)
                        VALUES (:table_name, :last_load_time,:message)
                    """), {'table_name': target_table_name, 'last_load_time': datetime.now(),'message':"new record added"})
            
            db.commit()
            log_update(tablename,scd_type)
            return {"message": "Record created successfully (SCD Type 2)"}
    

        
