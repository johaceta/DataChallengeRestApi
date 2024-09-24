from fastapi import FastAPI
from pydantic import BaseModel
from tkinter import Tk, filedialog
from typing import List, Dict
import uvicorn 
import pandas as pd
import fastavro
import pyodbc
import os


app = FastAPI()

# Define a model for the database connection credentials
class DBConnectionDetails(BaseModel):
    server: str
    database: str
    uid: str
    pwd: str

# Define models for each table
class Employee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str
class ViewQuery(BaseModel):
    view_name: str
    db_credentials: DBConnectionDetails 

#Connect to database function
def get_db_connection(credentials: DBConnectionDetails):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={credentials.server};"
            f"DATABASE={credentials.database};"
            f"UID={credentials.uid};"
            f"PWD={credentials.pwd};"
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error connecting to database: {str(e)}")

# Insert Functions
def insert_employees(entries: List[Employee], credentials: DBConnectionDetails):
    conn = get_db_connection(credentials)
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
            VALUES (?, ?, ?, ?, ?)
        """, [(entry.id, entry.name, entry.datetime, entry.department_id, entry.job_id) for entry in entries])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error inserting employee data: {str(e)}")
    finally:
        conn.close()

def insert_departments(entries: List[Department], credentials: DBConnectionDetails):
    conn = get_db_connection(credentials)
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO departments (id, department)
            VALUES (?, ?)
        """, [(entry.id, entry.department) for entry in entries])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error inserting department data: {str(e)}")
    finally:
        conn.close()

def insert_jobs(entries: List[Job], credentials: DBConnectionDetails):
    conn = get_db_connection(credentials)
    cursor = conn.cursor()
    try:
        cursor.executemany("""
            INSERT INTO jobs (id, job)
            VALUES (?, ?)
        """, [(entry.id, entry.job) for entry in entries])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Error inserting job data: {str(e)}")
    finally:
        conn.close()

        
def backup_location(table_name: str) -> str:
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.asksaveasfilename(
        title="Select location to save the backup",
        initialfile=f"{table_name}_backup.avro",
        defaultextension=".avro",
        filetypes=[("AVRO files", "*.avro")]
    )
    return file_path

# Function to get the backup file location (for local use)
def get_backup_file_location() -> str:
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(
        title="Select AVRO file to restore",
        defaultextension=".avro",
        filetypes=[("AVRO files", "*.avro")]
    )
    return file_path

# Function to extract table name from file path
def extract_table_name(file_path: str):
    file_name = os.path.basename(file_path)
    if not file_name.endswith('_backup.avro'):
        raise ValueError(f"Invalid file format. Expected format: {{table_name}}_backup.avro")
    
    table_name = file_name.replace('_backup.avro', '')
    return table_name

# Function to create the specific table if it doesn't exist
def create_table_if_not_exist(conn, table_name: str):
    cursor = conn.cursor()
    
    # SQL query to check if the table exists
    check_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
    BEGIN
    """

    # Add table creation based on the table name
    if table_name == "departments":
        create_table_query = '''CREATE TABLE departments (
                                    id INTEGER ,
                                    department VARCHAR(50) NOT NULL
                                );'''
    elif table_name == "jobs":
        create_table_query = '''CREATE TABLE jobs (
                                    id INTEGER ,
                                    job VARCHAR(50) NOT NULL
                                );'''
    elif table_name == "hired_employees":
        create_table_query = '''CREATE TABLE hired_employees (
                                    id INTEGER ,
                                    name TEXT NOT NULL,
                                    datetime TEXT NOT NULL,
                                    department_id INTEGER,
                                    job_id INTEGER
                                );'''
    else:
        raise ValueError(f"Unknown table name: {table_name}")

    # Final query combining the check and the creation
    full_query = check_table_query + create_table_query + " END;"
    
    # Execute the query
    cursor.execute(full_query)
    conn.commit()

# Backup function
def backup_table(table_name: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        print(f"Executing query: {query}")
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]  # Get column names
        rows = cursor.fetchall()  # Fetch all rows

        if not rows:
            raise ValueError(f"No data found in table {table_name}")

        # Create DataFrame from rows
        df = pd.DataFrame.from_records(rows, columns=columns)

        type_mapping = {
            "int64": "int",
            "float64": "float",
            "bool": "boolean",
            "object": "string",  
            "datetime64[ns]": "string",  
            "timedelta64[ns]": "string"  
        }
        schema = {
            "type": "record",
            "name": table_name,
            "fields": [
                {"name": col, "type": type_mapping.get(str(dtype), "string")}
                for col, dtype in df.dtypes.items()
            ]
        }

        # Prompt user for backup location
        file_path = backup_location(table_name)
        if not file_path:
            raise ValueError("No file path was selected")
        
        try:
            with open(file_path, 'wb') as out:
                fastavro.writer(out, schema, df.to_dict('records'))
        except Exception as e:
            print(f"Error writing file: {e}")
            raise HTTPException(status_code=500, detail=f"Error writing file: {str(e)}")

        return file_path
    
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

#Restore Function
def restore_table():
    conn = get_db_connection()
    try:
        # Construct the file path
        file_path = get_backup_file_location()
        
        # Check if the file exists
        if not os.path.isfile(file_path):
            raise ValueError(f"File not found: {file_path}")
        
        # Extract the table name from the file name
        table_name = extract_table_name(file_path)

        # Create table if it doesn't exist
        create_table_if_not_exist(conn, table_name)

        # Read the AVRO file
        with open(file_path, 'rb') as in_file:
            reader = fastavro.reader(in_file)
            records = [record for record in reader]
        
        if not records:
            raise ValueError(f"No data found in file {file_path}")

        # Create DataFrame from records
        df = pd.DataFrame(records)
        
        # Validate DataFrame
        if df.empty:
            raise ValueError(f"No data to restore from file {file_path}")
        
        # Get table schema and column names
        column_names = df.columns.tolist()
        columns_placeholder = ', '.join(f'"{col}"' for col in column_names)
        values_placeholder = ', '.join('?' * len(column_names))

        # Create insert query
        insert_query = f"INSERT INTO {table_name} ({columns_placeholder}) VALUES ({values_placeholder})"

        # Insert data into the database
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()

        return {"status": "Restore successful"}

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


#API's end point

@app.post("/insert/{table_name}")
def insert_data(table_name: str, entries: List[dict], db_credentials: DBConnectionDetails):
    if table_name == "hired_employees":
        try:
            employee_entries = [Employee(**entry) for entry in entries]
            insert_employees(employee_entries, db_credentials)
            return {"status": "Inserted Employee data successfully"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    elif table_name == "departments":
        try:
            department_entries = [Department(**entry) for entry in entries]
            insert_departments(department_entries, db_credentials)
            return {"status": "Inserted Department data successfully"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    elif table_name == "jobs":
        try:
            job_entries = [Job(**entry) for entry in entries]
            insert_jobs(job_entries, db_credentials)
            return {"status": "Inserted Job data successfully"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    else:
        raise HTTPException(status_code=404, detail="Table not found")
   
@app.get("/backup/{table_name}")
def get_backup(table_name: str):
    try:
        file_path = backup_table(table_name)
        return {"status": "Backup completed", "file_path": file_path}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during backup: {str(e)}")
    
@app.post("/restore")
def restore():
    try:
        result = restore_table()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/report/query_view")
def query_view(view_query: ViewQuery):
    view_name = view_query.view_name
    db_credentials = view_query.db_credentials
    conn = get_db_connection(db_credentials)
    cursor = conn.cursor()

    query = ""
    # Add special handling for specific views that need extra clauses
    if view_name == "vw_HiredNumberOverMean2021":
        query = f"SELECT * FROM {view_name} ORDER BY numberHired DESC"
    else:
        query = f"SELECT * FROM {view_name}"

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

        result = [dict(zip(columns, row)) for row in rows]
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")
    finally:
        conn.close()

if __name__== "__main__":
    uvicorn.run(app, host="127.0.0.1", port= 9000)

