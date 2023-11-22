import pyodbc

def execute_sql_script(connection_string, sql_script, params=None) -> dict:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(sql_script, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip([column[0] for column in cursor.description], row)) for row in rows]

def get_sql_table_schema(sql_connection_string: str, table_name:str) -> dict:

    query = "SELECT COLUMN_NAME AS 'name', DATA_TYPE AS 'type' FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?"

    return execute_sql_script(sql_connection_string, query, [table_name])
