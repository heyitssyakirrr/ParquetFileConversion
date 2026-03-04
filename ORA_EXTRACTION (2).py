import pandas as pd
import cx_Oracle
from Data_Warehouse.Common.Jobs.ORA_CON import create_ora_con_srcdw,create_ora_con_pbbdw,close_ora_con

batch_size = 100000
def exct_srcdwh_monthly(table_name,field_name,start_date,end_date):
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from srcdw.{table_name}
            where {field_name} between TO_TIMESTAMP('{start_date}','YYYY-MM-DD HH24:MI:SS') and  TO_TIMESTAMP('{end_date}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)
        
        for col in out_df.select_dtypes(include=['object']).columns:
            out_df[col] = out_df[col].apply(lambda x: str(x) if isinstance(x, cx_Oracle.LOB) else x)
        
        cursor.close()

        return out_df
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_srcdw)
        

def exct_srcdwh_daily(table_name,field_name,date):
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from srcdw.{table_name}
            where TRUNC({field_name}) = TRUNC(TO_DATE('{date}', 'YYYY-MM-DD HH24:MI:SS'))
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)

        for col in out_df.select_dtypes(include=['object']).columns:
            out_df[col] = out_df[col].apply(lambda x: str(x) if isinstance(x, cx_Oracle.LOB) else x)
        
        cursor.close()

        return out_df
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_srcdw)

def exct_srcdw_full_base(table_name):
    try:
        connect_srcdw = create_ora_con_srcdw()
        cursor = connect_srcdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from srcdw.{table_name}
        """
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)

        cursor.close()
       
        return out_df
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_srcdw)

def exct_pbbdw_full_base(table_name):
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from pbbdw.{table_name}
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)
        
        cursor.close()
        
        return out_df
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_pbbdw)


def exct_pbbdw_monthly(table_name,field_name,start_date,end_date):
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from pbbdw.{table_name}
            where {field_name} between TO_TIMESTAMP('{start_date}','YYYY-MM-DD HH24:MI:SS') and  TO_TIMESTAMP('{end_date}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)

        for col in out_df.select_dtypes(include=['object']).columns:
            out_df[col] = out_df[col].apply(lambda x: str(x) if isinstance(x, cx_Oracle.LOB) else x)

        cursor.close()
        
        return out_df
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_pbbdw)

def exct_pbbdwh_daily(table_name,field_name,date):
    try:
        connect_pbbdw = create_ora_con_pbbdw()
        cursor = connect_pbbdw.cursor() 
        cursor.arraysize=batch_size

        query = f"""
            SELECT *
            from pbbdw.{table_name}
            where TRUNC({field_name}) = TRUNC(TO_DATE('{date}', 'YYYY-MM-DD HH24:MI:SS'))
        """

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)

        for col in out_df.select_dtypes(include=['object']).columns:
            out_df[col] = out_df[col].apply(lambda x: str(x) if isinstance(x, cx_Oracle.LOB) else x)

        cursor.close()
        
        return out_df
    
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()

    finally:
        close_ora_con(connect_pbbdw)

def exct_srcdw_query(query):
    try:
        connect_srcdw = create_ora_con_srcdw()       
        cursor = connect_srcdw.cursor() 
        cursor.arraysize=batch_size

        cursor.execute(query)
        columns = [col[0] for col in cursor.description]

        all_row = []
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            all_row.extend(rows)

        out_df = pd.DataFrame(all_row, columns=columns)
        
        cursor.close()
        
        return out_df
    except cx_Oracle.DatabaseError as e:
        print(f"Database error occourred: {e}")
        return pd.DataFrame()
    finally:
        close_ora_con(connect_srcdw)



