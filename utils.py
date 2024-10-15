from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

# def create_target_table_if_not_exists(target_table_name: str, target_columns: list,scd_type:str, db: Session):
#    # Fetch the primary key and surrogate key columns from SCD_Entities
#    result = db.execute(text("""
#        SELECT Target_Primary_Key_Columns, Target_Surrogate_Key_Column FROM SCD_Entities
#        WHERE Target_Table_Name = :target_table_name
#    """), {'target_table_name': target_table_name}).fetchone()
#    print('Result:', result)
#    if not result:
#        raise HTTPException(status_code=404, detail="Primary key or surrogate key not found in metadata")
#    primary_key_column, surrogate_key_column = result
#    # Construct the CREATE TABLE statement dynamically
#    columns_definition = ", ".join([f"{col} VARCHAR(255)" for col in target_columns if col not in [primary_key_column, surrogate_key_column]])
#    print('Primary Key:', primary_key_column, 'Surrogate Key:', surrogate_key_column)
#    print("columns_definition",columns_definition)
#    create_table_query = f"""
#    IF OBJECT_ID('{target_table_name}', 'U') IS NULL
#    BEGIN
#        CREATE TABLE {target_table_name} (
#            {surrogate_key_column} INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key as primary
#            {primary_key_column} VARCHAR(255),  -- Business key (e.g., CustomerID)
#            {columns_definition}
#        )
#    END
#    """
#    db.execute(text(create_table_query))
#    db.commit()
#    print(f"Target table '{target_table_name}' created successfully.")
def create_target_table_if_not_exists(target_table_name: str, target_columns: list, scd_type: str, db: Session):
    # Fetch the primary key and surrogate key columns from SCD_Entities
    result = db.execute(text("""
        SELECT Target_Primary_Key_Columns, Target_Surrogate_Key_Column FROM SCD_Entities
        WHERE Target_Table_Name = :target_table_name
    """), {'target_table_name': target_table_name}).fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Primary key or surrogate key not found in metadata")
    
    primary_key_column, surrogate_key_column = result
    
    # Construct the CREATE TABLE statement dynamically
    columns_definition = ", ".join([f"{col} VARCHAR(255)" for col in target_columns if col not in [primary_key_column, surrogate_key_column, 'StartDate', 'EndDate', 'IsCurrent', 'UpdatedOn']])
    
    if scd_type == 'Type 2':
        # Add SCD Type 2 specific columns
        columns_definition += ", StartDate DATETIME, EndDate DATETIME, IsCurrent BIT"
    
    create_table_query = f"""
    IF OBJECT_ID('{target_table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {target_table_name} (
            {surrogate_key_column} INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate key as primary
            {primary_key_column} VARCHAR(255),  -- Business key (e.g., CustomerID)
            {columns_definition},
            UpdatedOn DATETIME
        )
    END
    """
    
    db.execute(text(create_table_query))
    db.commit()
    print(f"Target table '{target_table_name}' created successfully.")


def initial_load_from_source_to_target(source_table: str, target_table: str, scd_type: str, db: Session):
   # Fetch the relevant columns from Table_Columns_Metadata
   metadata_columns = db.execute(text("""
       SELECT Column_Name FROM Table_Columns_Metadata
       WHERE Table_Name = :source_table AND Is_Target_Column = 1
   """), {'source_table': source_table}).fetchall()

   print("Metadata Columns:", metadata_columns)

   # Extract column names from tuples
   target_columns = [row[0] for row in metadata_columns]
   print("Target Columns:", target_columns)

   # Fetch column names from the source table's metadata
   source_columns_query = text(f"""
       SELECT COLUMN_NAME
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = :source_table
   """)
   source_columns_result = db.execute(source_columns_query, {'source_table': source_table}).fetchall()
   source_columns = [row[0] for row in source_columns_result]
   print("Source Columns:", source_columns)

   # Filter target columns to include only those present in the source table
   valid_columns = [col for col in target_columns if col in source_columns]
   print("Valid Columns:", valid_columns)

   # Fetch records from the source table
   source_records = db.execute(text(f"""
       SELECT {', '.join(valid_columns)} FROM {source_table} WHERE UpdatedOn IS NOT NULL
   """)).fetchall()
#    print("Source Records:", source_records)

   for record in source_records:
       record_dict = dict(record._mapping)
       if scd_type == 'Type 2':
           # Set StartDate, EndDate, and IsCurrent for SCD Type 2
           record_dict['StartDate'] = record_dict.get('UpdatedOn')
           record_dict['EndDate'] = None  # Default value for new records
           record_dict['IsCurrent'] = 1    # Mark the new record as current

       else:  
           # Handle other SCD types as needed
           # Remove date and current indicators if needed (for Type 1)

           record_dict.pop('StartDate', None)
           record_dict.pop('EndDate', None)
           record_dict.pop('IsCurrent', None)

    #    print("Record Dictionary:", record_dict)
       columns = ', '.join(record_dict.keys())
       values = ', '.join([f":{col}" for col in record_dict.keys()])
       # Prepare the SQL query
       sql_query = f"""
           INSERT INTO {target_table} ({columns})
           VALUES ({values})
       """
    #    print(f"SQL Query: {sql_query}")
    #    print(f"Parameters: {record_dict}")
       try:
           db.execute(text(sql_query), record_dict)
       except Exception as e:
           print(f"Error executing query: {e}")
   db.commit()
   print(f"Initial load from {source_table} to {target_table} completed successfully.")
