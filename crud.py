from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from .utils import create_target_table_if_not_exists, initial_load_from_source_to_target


def create_record(tablename:str,record: dict, db: Session):
    # Fetch the relevant columns from the source table's metadata
    source_columns = db.execute(text(f"""
        SELECT COLUMN_NAME, COLUMNPROPERTY(OBJECT_ID(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IsIdentity
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
    """), {'table_name': tablename}).fetchall()
    print("source_columns",source_columns)

    # Uses the COLUMNPROPERTY function to check if the column is an identity column. It returns 1 if the column is an identity column, otherwise 0.

    # Filter out identity columns
    non_identity_columns = [row[0] for row in source_columns if row[1] != 1]
    print("non_identity_columns",non_identity_columns)

    columns=', '.join(non_identity_columns)
    print("columns",columns)
    values=', '.join([f':{col}' for col in non_identity_columns])
    print("values",values)

    query=text(f"""INSERT INTO {tablename} ({columns}) VALUES ({values})""")
    print("query",query)

    # Add time stamps
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
    # print('Primary Key  Column .............',primary_key_column)

    # Fetch the relevant columns from Table_Columns_Metadata
    metadata_columns = db.execute(text("""
        SELECT Column_Name FROM Table_Columns_Metadata
        WHERE Table_Name = :tablename AND Is_Target_Column = 1
    """), {'tablename': tablename}).fetchall()
    # print('MetaTable Column .............',metadata_columns)
    target_columns = [row[0] for row in metadata_columns]

    # Fetch the target table name from SCD_Entities
    target_table_row = db.execute(text("""
        SELECT Target_Table_Name FROM SCD_Entities
        WHERE Source_Table_Name = :tablename
    """), {'tablename': tablename}).fetchone()
    # print('Target  Column .............',target_table_row)
    if not target_table_row:
        raise HTTPException(status_code=404, detail="Target table name not found")
    
    target_table_name = target_table_row[0]
    
    # # Check and create target table if necessary
    create_target_table_if_not_exists(target_table_name, target_columns, db)

    # Perform initial load from source to target table if target table is empty
    target_table_count = db.execute(text(f"SELECT COUNT(*) FROM {target_table_name}")).scalar()
    
    if target_table_count == 0:
        initial_load_from_source_to_target(tablename, target_table_name, db)
    print("target_table_count",target_table_count)


    # Fetch the most recent record from the target table
    existing_record = db.execute(text(f"""
        SELECT TOP 1 * FROM {target_table_name} WHERE {primary_key_column} = :{primary_key_column} ORDER BY UpdatedOn DESC
    """), {primary_key_column: new_record[primary_key_column]}).fetchone()
    

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

            # Prepare new record for insertion
            new_row = {col: new_record.get(col, existing_record[col]) for col in target_columns}
            new_row[primary_key_column] = new_record[primary_key_column]
            new_row['StartDate'] = datetime.now()
            new_row['EndDate'] = None
            new_row['UpdatedOn'] = datetime.now()
            new_row['IsCurrent'] = 1

            columns = ', '.join(new_row.keys())
            values = ', '.join([f":{col}" for col in new_row.keys()])
            db.execute(text(f"""
                INSERT INTO {target_table_name} ({columns})
                VALUES ({values})
            """), new_row)
            db.commit()
            return {"message": "Record updated successfully in target table"}
        else:
            return {"message": "No changes detected"}
    else:
        # If no existing record is found, insert the new record directly
        new_row = {col: new_record.get(col, None) for col in target_columns}
        new_row[primary_key_column] = new_record[primary_key_column]
        new_row['StartDate'] = datetime.now()
        new_row['EndDate'] = None
        new_row['UpdatedOn'] = datetime.now()
        new_row['IsCurrent'] = 1

        columns = ', '.join(new_row.keys())
        values = ', '.join([f":{col}" for col in new_row.keys()])

        db.execute(text(f"""
            INSERT INTO {target_table_name} ({columns})
            VALUES ({values})
        """), new_row)
        
        db.commit()
        return {"message": "Record created successfully in target table"}
