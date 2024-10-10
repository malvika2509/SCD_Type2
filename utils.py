from sqlalchemy import text
from sqlalchemy.orm import Session

def create_target_table_if_not_exists(target_table_name: str, target_columns: list, db: Session):
    # Define fixed columns to exclude from dynamic columns
    fixed_columns = {"CustomerKey","CustomerID", "StartDate", "EndDate", "UpdatedOn", "IsCurrent"}
    
    # Filter out fixed columns from target columns
    dynamic_columns = [col for col in target_columns if col not in fixed_columns]
    print('dynamic_columns',dynamic_columns)
    
    # Construct the CREATE TABLE statement dynamically
    columns_definition = ", ".join([f"{col} VARCHAR(255)" for col in dynamic_columns])
    create_table_query = f"""
    IF OBJECT_ID('{target_table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {target_table_name} (
            CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
            CustomerID VARCHAR(255),
            {columns_definition},
            StartDate DATETIME,
            EndDate DATETIME,
            UpdatedOn DATETIME,
            IsCurrent BIT
        )
    END
    """
    print("Target table created successfully")
    db.execute(text(create_table_query))
    db.commit()

def initial_load_from_source_to_target(source_table: str, target_table: str, db: Session):
    # Fetch the relevant columns from Table_Columns_Metadata
    metadata_columns = db.execute(text("""
        SELECT Column_Name FROM Table_Columns_Metadata
        WHERE Table_Name = :source_table AND Is_Target_Column = 1
    """), {'source_table': source_table}).fetchall()

    print("metadata_columns_initial",metadata_columns)

    # Extract column names from tuples
    target_columns = [row[0] for row in metadata_columns]
    print("target_columns_initial",target_columns)


    # Fetch column names from the source table's metadata
    source_columns_query = text(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :source_table
    """)
    source_columns_result = db.execute(source_columns_query, {'source_table': source_table}).fetchall()
    source_columns = [row[0] for row in source_columns_result]
    print("source_columns_result",source_columns_result)
    print("source_columns",source_columns)

    # Filter target columns to include only those present in the source table
    valid_columns = [col for col in target_columns if col in source_columns]
    print("valid_columns",valid_columns)

    # Fetch records from the source table
    source_records = db.execute(text(f"""
        SELECT {', '.join(valid_columns)} FROM {source_table} WHERE UpdatedOn IS NOT NULL
    """)).fetchall()

    print("source_records", source_records)

    for record in source_records:
        record_dict = dict(record._mapping)
        record_dict['StartDate'] = record_dict['UpdatedOn']
        record_dict['EndDate'] = None
        record_dict['IsCurrent'] = 1

        print("record_dict", record_dict)

        columns = ', '.join(record_dict.keys())
        values = ', '.join([f":{col}" for col in record_dict.keys()])
        
        # Print the SQL query and parameters
        sql_query = f"""
            INSERT INTO {target_table} ({columns})
            VALUES ({values})
        """
        print(f"SQL Query: {sql_query}")
        print(f"Parameters: {record_dict}")

        try:
            db.execute(text(sql_query), record_dict)
        except Exception as e:
            print(f"Error executing query: {e}")

    db.commit()
