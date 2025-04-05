import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote

load_dotenv()

def get_connection():
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    # Encode the password to handle special characters like '@'
    encoded_password = quote(password)

    driver = 'ODBC Driver 18 for SQL Server'
    driver_encoded = driver.replace(' ', '+')

    connection_url = (
        f"mssql+pyodbc://{username}:{encoded_password}@{server}:1433/{database}?"
        f"driver={driver_encoded}"
        f"&Encrypt=yes"
        f"&TrustServerCertificate=no"
    )

    try:
        engine = create_engine(connection_url)
        engine.connect()
        print("Connection successful")
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_table_list(engine):
    query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME LIKE '20%'
    """
    table_list = pd.read_sql(query, engine)
    return table_list['TABLE_NAME'].tolist()

def import_table_data(engine, table_name):
    query = f"SELECT * FROM [{table_name}]"
    df = pd.read_sql(query, engine)
    return df

def import_all_tables(engine):
    table_list = get_table_list(engine)
    data_dict = {}

    for table_name in table_list:
        df = import_table_data(engine, table_name)
        data_dict[table_name] = df

    return data_dict

if __name__ == "__main__":
    engine = get_connection()
    if engine:
        data_dict = import_all_tables(engine)
        print("Imported tables:")
        for table_name in data_dict.keys():
            print(table_name)
        engine.dispose()  # Properly close the SQLAlchemy engine
    else:
        print("Failed to establish a database connection.")