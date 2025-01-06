import numpy as np 
import pandas as pd 
from sqlalchemy import create_engine
from fastavro import writer, parse_schema, read

# globally define the connection
user_name = 'postgres'
password = 'Ducanh712'
ip = '35.188.10.153'
database_name = 'northwind-db'
connection_string = (
    f"postgresql+psycopg2://{user_name}:{password}@{ip}:5432/{database_name}"
)
engine = create_engine(connection_string)

with engine.connect() as conn:
    query='SELECT * FROM employees'
    df = pd.read_sql(query, conn)

print(f'Original data types: {df.dtypes}')

def is_datetime_column(df, col):
    """
    Check if a column is datetime-like. Avoid misidentifying sparse columns as datetime.
    """
    non_null_values = df[col].dropna()  # Ignore nulls during the check
    if non_null_values.empty:  # If all values are null, it can't be datetime
        return False

    # Attempt to convert the non-null values to datetime
    try:
        pd.to_datetime(non_null_values, errors='raise')  # Raise error for invalid dates
        return True  # If conversion succeeds, it's a datetime column
    except (ValueError, TypeError):
        return False

# Function to process and convert datetime columns to strings
def process_dataframe(df, original_types):
    for col in df.columns:
        if original_types[col] == 'object' and is_datetime_column(df, col):
            # Explicitly convert datetime-like columns to string format
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')  # Format as ISO 8601 string
        elif original_types[col] == 'object':  # Ensure object columns stay as strings
            df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '': None})
        elif original_types[col] == 'datetime64[ns]':  # Convert datetime64[ns] to string
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S')  # ISO 8601 format
    return df


# Function to generate Avro schema from a Pandas DataFrame
def get_avro_schema(df, data_type_to_avro, table_name, namespace="Northwind-OLTP", doc="Extracted from OLTP"):

    schema = {
        "doc": doc,                # Description of the schema
        "name": table_name,       # The name of the record
        "namespace": namespace,    # Namespace for the schema
        "type": "record",          # Type of Avro schema: 'record'
        "fields": []               # List of fields
    }
    
    # Add fields based on the DataFrame columns and their types
    for col in df.columns:
        dtype = df[col].dtype
        avro_type = data_type_to_avro.get(str(dtype), 'string')  # Default to 'string' for unknown types
        
        # Check if the column has null values
        if df[col].isnull().any():
            # Allow null in the field
            avro_type = ["null", avro_type]  # Avro allows unions of types

        # Append field info to the schema
        schema['fields'].append({
            "name": col,             # Column name
            "type": avro_type        # Mapped Avro type
        })
    
    # Parse and return the schema using fastavro's parse_schema function
    return parse_schema(schema)

dtype_to_avro = {
    'object': 'string',         # 'object' -> string
    'int64': 'long',            # 'int64' -> long
    'float64': 'double',        # 'float64' -> double
    'datetime64[ns]': 'string', # 'datetime64[ns]' -> string (or 'long' if you want Unix timestamp)
    'bool': 'boolean',          # 'bool' -> boolean
    'category': 'string',       # 'category' -> string
}

# Load data from SQL
with engine.connect() as conn:
    query = 'SELECT * FROM employees'
    df = pd.read_sql(query, conn)

# Track original types
original_types = df.dtypes

# Process the dataframe to ensure datetime columns are formatted correctly
process_dataframe(df, original_types)

# Generate the Avro schema
parsed_schema = get_avro_schema(df, dtype_to_avro, 'employees')

print(f'This is the parsed schema: {parsed_schema}')

# Writing to Avro
with open('employees.avro', 'wb') as out:
    records = df.to_dict(orient='records')

    for record in records:
        for key, value in record.items():
            if pd.isna(value):  # Catch NaT explicitly using pd.isna()
                record[key] = None
    
    writer(out, parsed_schema, records)