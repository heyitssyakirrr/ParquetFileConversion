import cx_Oracle
import pyarrow as pa
import duckdb
from Data_Warehouse.Common.Jobs.ORA_CON import create_ora_con_srcdw, create_ora_con_pbbdw, close_ora_con

batch_size = 500000  # Increased for faster fetches

def _output_type_handler(cursor, name, default_type, size, precision, scale):
    """Handle LOBs as strings instead of cx_Oracle.LOB objects"""
    if default_type == cx_Oracle.CLOB or default_type == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.STRING, size=size or 4000, arraysize=cursor.arraysize)
    return None

def _fetch_and_convert_to_arrow(cursor, columns):
    """Fetch all rows and convert to PyArrow Table"""
    all_rows = []
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        all_rows.extend(rows)
    
    # Convert to PyArrow Table directly (no pandas)
    schema = pa.schema([(col, pa.string()) for col in columns])
    table = pa.Table.from_pylist(
        [dict(zip(columns, row)) for row in all_rows],
        schema=schema
    )
    return table

def exct_srcdwh_monthly(table_name, field_name, start_date, end_date):
    """Extract monthly data from SRCDW and return as PyArrow Table"""
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM srcdw.{table_name}
            WHERE {field_name} BETWEEN TO_TIMESTAMP('{start_date}','YYYY-MM-DD HH24:MI:SS')
                                 AND TO_TIMESTAMP('{end_date}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
        
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_srcdw)


def exct_srcdwh_daily(table_name, field_name, date):
    """Extract daily data from SRCDW and return as PyArrow Table"""
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM srcdw.{table_name}
            WHERE TRUNC({field_name}) = TRUNC(TO_DATE('{date}', 'YYYY-MM-DD HH24:MI:SS'))
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()

        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_srcdw)


def exct_srcdw_full_base(table_name):
    """Extract full table from SRCDW and return as PyArrow Table"""
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM srcdw.{table_name}
        """
        
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
       
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_srcdw)


def exct_pbbdw_full_base(table_name):
    """Extract full table from PBBDW and return as PyArrow Table"""
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM pbbdw.{table_name}
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
        
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_pbbdw)


def exct_pbbdw_monthly(table_name, field_name, start_date, end_date):
    """Extract monthly data from PBBDW and return as PyArrow Table"""
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM pbbdw.{table_name}
            WHERE {field_name} BETWEEN TO_TIMESTAMP('{start_date}','YYYY-MM-DD HH24:MI:SS')
                                 AND TO_TIMESTAMP('{end_date}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
        
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_pbbdw)


def exct_pbbdwh_daily(table_name, field_name, date):
    """Extract daily data from PBBDW and return as PyArrow Table"""
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        query = f"""
            SELECT *
            FROM pbbdw.{table_name}
            WHERE TRUNC({field_name}) = TRUNC(TO_DATE('{date}', 'YYYY-MM-DD HH24:MI:SS'))
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
        
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_pbbdw)


def exct_srcdw_query(query):
    """Execute custom query on SRCDW and return as PyArrow Table"""
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor()
        cursor.arraysize = batch_size
        cursor.outputtypehandler = _output_type_handler

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        table = _fetch_and_convert_to_arrow(cursor, columns)
        cursor.close()
        
        return table
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occurred: {e}")
        return pa.Table.from_pylist([], schema=pa.schema([]))
    finally:
        close_ora_con(connect_srcdw)


# Optional: Helper functions for saving to parquet using DuckDB

def save_table_to_parquet(table, output_file):
    """Save PyArrow Table to parquet using DuckDB"""
    conn = duckdb.connect()
    conn.register("temp_table", table)
    conn.execute(f"COPY temp_table TO '{output_file}' (FORMAT PARQUET)")
    conn.close()
    print(f"[INFO] Saved table to {output_file}")


def read_parquet_with_duckdb(file_path):
    """Read parquet file using DuckDB and return as PyArrow Table"""
    conn = duckdb.connect()
    result = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_arrow_table()
    conn.close()
    return result