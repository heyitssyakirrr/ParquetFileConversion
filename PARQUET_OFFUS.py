import os
from datetime import datetime
from pathlib import Path
import re
import pyarrow.parquet as pq
import pyarrow as pa
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import shutil
import glob
import concurrent.futures
from Data_Warehouse.Common.Jobs.ORA_CON import *  # Custom Oracle connection
from Data_Warehouse.Common.Jobs.ORA_EXTRACTION import *
from Data_Warehouse.Common.Jobs.GET_BATCH_DATE import *

def main():
    # === LOGGING ===
    start_time = datetime.now()
    print(f"[INFO] Program started at: {start_time}", flush=True)

    # === CONFIGURATION ===

    # === TABLE CONFIG ===
    tablename = "DP_RPP_OFFUS"
    # start_valid_dttm = get_batch_date_mart('DP')
    # end_valid_dttm = last_date_of_batch_date(start_valid_dttm)
    start_valid_dttm = '2024-01-01 00:00:00'
    end_valid_dttm = '2024-12-31 23:59:59'
    valid_dt = format_date(start_valid_dttm, "%d%b%Y")
    valid_dttm = format_date(start_valid_dttm, "%d%b%Y:%H:%M:%S")
    field_name = "VALID_DTTM"
    batch_date_obj = datetime.strptime(start_valid_dttm,"%Y-%m-%d %H:%M:%S").date()
    datetime_now = pd.Timestamp.now().strftime("%d%b%Y:%H:%M:%S")
    file_dt = batch_date_obj.strftime('%Y%m%d')

    # === PARQUET CONFIG ===
    parquet_dir = "/parquet/batches/"
    output_file = f"{parquet_dir}{tablename}_{file_dt}_year2024.parquet"
    checkpoint_file = f"/parquet/checkpoint/{tablename}_{file_dt}_year2024_CHECKPOINT.txt"

    # === PARALLEL RUN CONFIG ===
    batch_size = 5_000_000  # Number of rows per batch
    merge_chunk_size = 20  # Number of batch files to merge at a time
    max_workers = 20  # Number of Dask workers
    memory_per_worker = '64GB'  # Set memory per worker to 32GB
    
    print(f"Table name : PBBDW.{tablename}", flush=True)

    # === CONNECT TO ORACLE ===
    def get_oracle_schema():
        """Fetches column names and data types from Oracle to ensure consistency."""
        connection = create_ora_con_pbbdw()
        cursor = connection.cursor()
        cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM ALL_TAB_COLUMNS
                    WHERE TABLE_NAME = '{tablename}'
                    AND OWNER = 'PBBDW'
                    ORDER BY COLUMN_ID
                    """)
        columns = []
        for col_name, data_type in cursor.fetchall():
            if "NUMBER" in data_type:
                pa_type = pa.float64()
            elif "VARCHAR" in data_type or "CHAR" in data_type:
                pa_type = pa.string()
            elif "DATE" in data_type or "TIMESTAMP" in data_type:
                pa_type = pa.timestamp('ns')
            else:
                pa_type = pa.string()
            
            columns.append((col_name, pa_type))
        
        cursor.close()
        connection.close()
        return pa.schema(columns)

    # Fetch Oracle schema for consistency
    print("[INFO] Fetching schema from Oracle...", flush=True)
    oracle_schema = get_oracle_schema()

    # === RESUME FROM CHECKPOINT ===
    last_completed_batch = -1
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            last_completed_batch = int(f.read().strip())

    print(f"[INFO] Resuming from batch {last_completed_batch + 1}", flush=True)

    # === DASK CLIENT SETUP ===
    client = Client(n_workers=max_workers, threads_per_worker=1, memory_limit=memory_per_worker)  # Set memory per worker to 8GB
    print(f"[INFO] Dask client started with {max_workers} workers and {memory_per_worker} memory limit per worker.", flush=True)

    # === EXTRACT DATA FROM ORACLE & SAVE AS PARQUET ===
    connection = create_ora_con_pbbdw()
    cursor = connection.cursor()
    cursor.execute(f"""SELECT *
                   FROM pbbdw.{tablename}
                   where {field_name} between TO_TIMESTAMP('{start_valid_dttm}','YYYY-MM-DD HH24:MI:SS') and  TO_TIMESTAMP('{end_valid_dttm}','YYYY-MM-DD HH24:MI:SS')""")

    def process_batch(batch_number, rows, cursor_description):
        """Process a batch: Convert to Parquet and save."""
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor_description])
        # df['VALID_DT'] = pd.to_datetime(valid_dt)
        # df['VALID_DTTM'] = pd.to_datetime(start_valid_dttm)
        # df['PROCESSED_DTTM'] = pd.Timestamp.now()
        print(df.dtypes)
        table = pa.Table.from_pandas(df,schema=oracle_schema)
        print(table.schema)
        parquet_file = f"{parquet_dir}{tablename}_year2024_batch{batch_number}.parquet"
        pq.write_table(table, parquet_file)
        # Save checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(batch_number))

        # Log every 1000 batches
        if batch_number % 10 == 0:
            print(f"[INFO] Completed {batch_number} at : {datetime.now()}", flush=True)

    # Process in parallel
    batch_number = 0
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            if batch_number <= last_completed_batch:
                batch_number += 1
                continue
            futures.append(executor.submit(process_batch, batch_number, rows, cursor.description))
            batch_number += 1
            concurrent.futures.wait(futures)

    cursor.close()
    connection.close()

    print("[INFO] Extraction complete. Merging Parquet files...", flush=True)

    # === MERGE PARQUET FILES INTO SINGLE FILE ===
    parquet_files = sorted([os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir)
                            if f.startswith(f"{tablename}_year2024_batch") and f.endswith(".parquet")])
    total_files = len(parquet_files)
    chunks = [parquet_files[i:i + merge_chunk_size] for i in range(0, total_files, merge_chunk_size)]

    def read_parquet_files(file_list):
        """Reads multiple Parquet files and ensures schema consistency."""
        tables = []
        for file in file_list:
            table = pq.read_table(file)
            table = table.cast(oracle_schema)  # Ensure consistent schema
            tables.append(table)
        return tables

    writer = None
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, chunk in enumerate(chunks):
            future = executor.submit(read_parquet_files, chunk)
            tables = future.result()
            combined_table = pa.concat_tables(tables)
            if writer is None:
                writer = pq.ParquetWriter(output_file, combined_table.schema)
            writer.write_table(combined_table)
            # Delete batch files after merging
            for file in chunk:
                os.remove(file)
            
        # print(f"[INFO] Merged and deleted {len(chunk)} batch files (Chunk {i + 1}/{len(chunks)})", flush=True)

    if writer:
        writer.close()

    # === PARQUET FILE VALIDATION ===
    def get_oracle_stat(tablename,field_name,start_valid_dttm,end_valid_dttm):
        connection = create_ora_con_pbbdw()
        cursor = connection.cursor()

        query = f"""
            SELECT COUNT(*) AS row_count,
            MIN({field_name}) AS min_valid_dttm,
            MAX({field_name}) AS max_valid_dttm
            FROM pbbdw.{tablename}
            where {field_name} between TO_TIMESTAMP('{start_valid_dttm}','YYYY-MM-DD HH24:MI:SS') and  TO_TIMESTAMP('{end_valid_dttm}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        row = cursor.fetchone()

        cursor.close()
        connection.close()

        return row

    def get_parquet_stat(output_file,valid_dttm_column = f"{field_name}"):
        table = pq.read_table(output_file,columns=[valid_dttm_column])

        df = table.to_pandas()

        row_count = len(df)
        min_val = df[valid_dttm_column].min()
        max_val = df[valid_dttm_column].max()

        return row_count, min_val, max_val
    
    def move_parquet_file(parquet_file_output):
        main_folder = Path("/parquet/current/DEPOSIT/")
        main_folder.mkdir(exist_ok=True)

        output_file_path = Path(parquet_file_output)

        match = re.search(r"(.+)_(\d{4})(\d{2})(\d{2})(\.parquet)", output_file_path.name)
        if not match:
            print(f"Error file name do not match pattern")
            return False
        name, year, month, day, extension = match.groups()

        target_dir = main_folder / f"year={year}"/ f"month={month}" /f"day={day}"

        target_dir.mkdir(parents=True, exist_ok=True)

        new_filename = f"{name}{extension}"

        target_path = target_dir / new_filename

        if target_path.exists():
            print(f"Error : Parquet File already exists - {target_path}")
            return False
        try:
            shutil.move(str(output_file_path), str(target_path))
            print(f"Successfully moved {new_filename} to {target_path}")
            return True
        except Exception as e:
            print(f"Error moving file : {e}")
            return False


    oracle_count, oracle_min, oracle_max = get_oracle_stat(tablename,field_name,start_valid_dttm,end_valid_dttm)
    print(f"Checking conversion for table : {tablename}",flush=True)
    print("Oracle statistics:",flush=True)
    print("Row count:", oracle_count,flush=True)
    print("min VALID_DTTM:", oracle_min,flush=True)
    print("max VALID_DTTM:", oracle_max,flush=True)

    parquet_count, parquet_min, parquet_max = get_parquet_stat(output_file)
    print("\nParquet statistics:",flush=True)
    print("Row count:", parquet_count,flush=True)
    print("min VALID_DTTM:", parquet_min,flush=True)
    print("max VALID_DTTM:", parquet_max,flush=True)

    # === VALIDATE AND MOVE TO FINAL FOLDER DELETE CHECKPOINT FILE ===
    if oracle_count == parquet_count and oracle_max== parquet_max and oracle_min == parquet_min:
        destination_path = "/parquet/historical"
        shutil.move(output_file,destination_path)
        os.remove(checkpoint_file)
        print("SUCCESS : Conversion complete and file moved",flush=True)
    else:
        print("FAILED : Please check again",flush=True)

    # === COMPLETION LOG ===
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print(f"[INFO] Merged {total_files} files into {output_file} successfully.", flush=True)
    print(f"[INFO] Program finished at: {end_time}", flush=True)
    print(f"[INFO] Total runtime: {elapsed_time}", flush=True)

if __name__ == "__main__":
    main()
