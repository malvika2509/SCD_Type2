from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session
from .utils import create_target_table_if_not_exists, initial_load_from_source_to_target

def create_record(record: dict, db: Session):
    query = text("""
        INSERT INTO DimCustomer (CustomerID, Name, Email, Address, City, State, Country, StartDate, EndDate, UpdatedOn, IsCurrent)
        VALUES (:CustomerID, :Name, :Email, :Address, :City, :State, :Country, :StartDate, NULL, :UpdatedOn, 1)
    """)
    record['StartDate'] = datetime.now()
    record['UpdatedOn'] = datetime.now()
    db.execute(query, record)
    db.commit()
    return {"message": "Record created successfully"}

def update_record(new_record: dict, db: Session):
    customer_id = new_record["CustomerID"]
    
    # Fetch the relevant columns from Table_Columns_Metadata
    metadata_columns = db.execute(text("""
        SELECT Column_Name FROM Table_Columns_Metadata
        WHERE Table_Name = 'DimCustomer' AND Is_Target_Column = 1
    """)).fetchall()

    print("metadata_columns",metadata_columns)

    # Extract column names from tuples
    target_columns = [row[0] for row in metadata_columns]

    print("target_columns",target_columns)

    # Fetch the target table name from SCD_Entities
    target_table = db.execute(text("""
        SELECT Target_Table_Name FROM SCD_Entities
        WHERE Source_Table_Name = 'DimCustomer'
    """)).fetchone()

    print("target_table",target_table)

    if not target_table:
        raise HTTPException(status_code=404, detail="Target table name not present")
    
    target_table_name = target_table[0]

    print("target_table_name",target_table_name)

    # Dynamically create the target table if it doesn't exist
    create_target_table_if_not_exists(target_table_name, target_columns, db)

    # Perform initial load from source to target table if target table is empty
    target_table_count = db.execute(text(f"SELECT COUNT(*) FROM {target_table_name}")).scalar()
    if target_table_count == 0:
        initial_load_from_source_to_target('DimCustomer', target_table_name, db)
    print("target_table_count",target_table_count)

    # Fetch the most recent record from the target table
    existing_record = db.execute(text(f"""
        SELECT TOP 1 * FROM {target_table_name} WHERE CustomerID = :customer_id ORDER BY UpdatedOn DESC
    """), {'customer_id': customer_id}).fetchone()

    print("existing_record",existing_record)

    if existing_record:
        existing_record = dict(existing_record._mapping)

        # Check for changes in the relevant columns
        changes_detected = any(
            existing_record[column] != new_record.get(column, existing_record[column])
            for column in target_columns
        )

        if changes_detected:
            # Update the previous record's EndDate and UpdatedOn
            db.execute(text(f"""
                UPDATE {target_table_name}
                SET EndDate = :end_date, UpdatedOn = :updated_on, IsCurrent = 0
                WHERE CustomerID = :customer_id AND IsCurrent = 1
            """), {'end_date': datetime.now(), 'updated_on': datetime.now(), 'customer_id': customer_id})

            # Insert the new record into the target table
            new_row = {col: new_record.get(col, existing_record[col]) for col in target_columns}
            new_row["CustomerID"] = customer_id
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

        # If no existing record is found, fetch from the source table
        source_record = db.execute(text(f"""
            SELECT * FROM DimCustomer WHERE CustomerID = :customer_id AND UpdatedOn IS NOT NULL
        """), {'customer_id': customer_id}).fetchone()

        if source_record:
            source_record = dict(source_record._mapping)
            source_record['StartDate'] = source_record['UpdatedOn']
            source_record['EndDate'] = None
            source_record['IsCurrent'] = 1

            columns = ', '.join(source_record.keys())
            values = ', '.join([f":{col}" for col in source_record.keys()])
            db.execute(text(f"""
                INSERT INTO {target_table_name} ({columns})
                VALUES ({values})
            """), source_record)

            db.commit()
            return {"message": "Record created successfully in target table from source"}
        else:
            # Insert the new record directly if it's a new CustomerID
            new_row = {col: new_record.get(col, None) for col in target_columns}
            new_row["CustomerID"] = customer_id
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
