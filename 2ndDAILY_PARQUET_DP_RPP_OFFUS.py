import os
from datetime import datetime
from pathlib import Path
import re
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
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
    start_valid_dttm = get_batch_date_mart('DP')
    end_valid_dttm = last_date_of_batch_date(start_valid_dttm)
    # start_valid_dttm = '2025-06-30 00:00:00'
    # end_valid_dttm = '2025-06-30 23:59:59'
    valid_dt = format_date(start_valid_dttm, "%d%b%Y")
    valid_dttm = format_date(start_valid_dttm, "%d%b%Y:%H:%M:%S")
    field_name = "VALID_DTTM"
    batch_date_obj = datetime.strptime(start_valid_dttm, "%Y-%m-%d %H:%M:%S").date()
    datetime_now = datetime.now().strftime("%d%b%Y:%H:%M:%S")
    file_dt = batch_date_obj.strftime('%Y%m%d')

    # === PARQUET CONFIG ===
    parquet_dir = "/parquet/batches/"
    output_file = f"{parquet_dir}{tablename}_{file_dt}.parquet"
    checkpoint_file = f"/parquet/checkpoint/{tablename}_{file_dt}_CHECKPOINT.txt"

    # === PARALLEL RUN CONFIG ===
    batch_size = 5_000_000  # Number of rows per batch
    merge_chunk_size = 20   # unused now but left for compatibility
    max_workers = 20        # Number of threads

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

    # === RESUME FROM CHECKPOINT (kept for compatibility/logging) ===
    # NOTE: We no longer rely on this for correctness under concurrency.
    last_completed_batch = -1
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            txt = f.read().strip()
            if txt:
                last_completed_batch = int(txt)

    print(f"[INFO] (Info only) checkpoint says last completed batch = {last_completed_batch}", flush=True)

    # Helper to standardize batch file paths
    def batch_paths(batch_number: int):
        parquet_file = os.path.join(parquet_dir, f"{tablename}batch{batch_number}.parquet")
        tmp_file = parquet_file + ".tmp"
        done_file = parquet_file + ".done"  # marker means "parquet_file is complete"
        return parquet_file, tmp_file, done_file

    # === EXTRACT DATA FROM ORACLE & SAVE AS PARQUET ===
    connection = create_ora_con_pbbdw()
    cursor = connection.cursor()
    cursor.execute(f"""SELECT *
                    FROM pbbdw.{tablename}
                    WHERE {field_name} between TO_TIMESTAMP('{start_valid_dttm}','YYYY-MM-DD HH24:MI:SS')
                    AND TO_TIMESTAMP('{end_valid_dttm}','YYYY-MM-DD HH24:MI:SS')""")

    def process_batch(batch_number, rows, cursor_description):
        """
        Process a batch: convert rows to Arrow/duckdb and write parquet safely.

        Crash-safety rules:
        - Write to .tmp first
        - Atomically rename .tmp -> .parquet
        - Create .done marker only after parquet file is finalized
        """
        parquet_file, tmp_file, done_file = batch_paths(batch_number)

        # If this batch was already completed in a previous run, skip.
        if os.path.exists(done_file):
            return

        col_names = [col[0] for col in cursor_description]

        # build an Arrow table directly from the list of tuples (no pandas)
        table = pa.Table.from_pylist(
            [dict(zip(col_names, row)) for row in rows],
            schema=oracle_schema
        )

        # If a previous run crashed mid-write, a leftover tmp may exist.
        # Remove it and rewrite cleanly.
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

        # use duckdb to write the table – no pandas or DataFrame involved
        conn = duckdb.connect()
        conn.register("batch_tbl", table)
        conn.execute(f"COPY batch_tbl TO '{tmp_file}' (FORMAT PARQUET)")
        conn.close()

        # Atomic rename: ensures we never leave a partial final parquet file behind.
        os.replace(tmp_file, parquet_file)

        # Marker file: indicates this batch parquet is complete and safe to merge.
        with open(done_file, "w") as f:
            f.write(datetime.now().isoformat())

        # (Optional) keep updating checkpoint for progress visibility only.
        # This is NOT used for correctness anymore.
        try:
            with open(checkpoint_file, "w") as f:
                f.write(str(batch_number))
        except Exception:
            pass

        # Log every 10 batches
        if batch_number % 10 == 0:
            print(f"[INFO] Completed {batch_number} at : {datetime.now()}", flush=True)

    # Process in parallel
    # Bounded in-flight futures to avoid serialism + control memory
    MAX_INFLIGHT = max_workers * 2   # set to max_workers * 1 if memory is tight

    batch_number = 0
    futures = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            parquet_file, tmp_file, done_file = batch_paths(batch_number)

            # Skip processing if this batch was already completed previously
            # (still fetches rows to advance the cursor, by design).
            if os.path.exists(done_file):
                batch_number += 1
                continue

            futures.add(executor.submit(process_batch, batch_number, rows, cursor.description))
            batch_number += 1

            # Only wait when we have "too many" tasks pending; wait for ONE to finish
            if len(futures) >= MAX_INFLIGHT:
                done, futures = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

    # Drain remaining tasks at the end
    concurrent.futures.wait(futures)

    cursor.close()
    connection.close()

    print("[INFO] Extraction complete. Merging Parquet files...", flush=True)

    # === MERGE ONLY COMPLETED BATCHES (.done present) ===
    done_pattern = os.path.join(parquet_dir, f"{tablename}batch*.parquet.done")
    done_markers = sorted(glob.glob(done_pattern))

    # Convert done markers to their parquet files
    batch_files = [d.replace(".done", "") for d in done_markers]

    if not batch_files:
        raise RuntimeError(f"No completed batches found to merge for {tablename} (no .done markers).")

    # Use duckdb to concatenate only completed batches into one output parquet
    con = duckdb.connect()
    files_sql = ",".join([f"'{p}'" for p in batch_files])
    con.execute(f"COPY (SELECT * FROM read_parquet([{files_sql}])) TO '{output_file}' (FORMAT PARQUET)")
    con.close()

    # Remove only merged batch files + their done markers
    for p in batch_files:
        if os.path.exists(p):
            os.remove(p)
    for d in done_markers:
        if os.path.exists(d):
            os.remove(d)

    # Also clean up any leftover tmp files from crashes (safe housekeeping)
    tmp_pattern = os.path.join(parquet_dir, f"{tablename}batch*.parquet.tmp")
    for t in glob.glob(tmp_pattern):
        try:
            os.remove(t)
        except Exception:
            pass

    # === PARQUET FILE VALIDATION ===
    def get_oracle_stat(tablename, field_name, start_valid_dttm, end_valid_dttm):
        connection = create_ora_con_pbbdw()
        cursor = connection.cursor()

        query = f"""
            SELECT COUNT(*) AS row_count,
            MIN({field_name}) AS min_valid_dttm,
            MAX({field_name}) AS max_valid_dttm
            FROM pbbdw.{tablename}
            where {field_name} between TO_TIMESTAMP('{start_valid_dttm}','YYYY-MM-DD HH24:MI:SS')
                                and TO_TIMESTAMP('{end_valid_dttm}','YYYY-MM-DD HH24:MI:SS')
        """

        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()
        connection.close()
        return row

    def get_parquet_stat(output_file, valid_dttm_column=field_name):
        con = duckdb.connect()
        con.execute(f"""
            SELECT COUNT(*) AS row_count,
                   MIN({valid_dttm_column}) AS min_val,
                   MAX({valid_dttm_column}) AS max_val
            FROM read_parquet('{output_file}')
        """)
        row = con.fetchone()
        con.close()
        return row

    def move_parquet_file(parquet_file_output):
        main_folder = Path("/parquet/current/DEPOSIT/")
        main_folder.mkdir(exist_ok=True)

        output_file_path = Path(parquet_file_output)

        match = re.search(r"(.+)_(\d{4})(\d{2})(\d{2})(\.parquet)", output_file_path.name)
        if not match:
            print(f"Error file name do not match pattern")
            return False
        name, year, month, day, extension = match.groups()

        target_dir = main_folder / f"year={year}" / f"month={month}" / f"day={day}"
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

    oracle_count, oracle_min, oracle_max = get_oracle_stat(tablename, field_name, start_valid_dttm, end_valid_dttm)
    print(f"Checking conversion for table : {tablename}", flush=True)
    print("Oracle statistics:", flush=True)
    print("Row count:", oracle_count, flush=True)
    print("min VALID_DTTM:", oracle_min, flush=True)
    print("max VALID_DTTM:", oracle_max, flush=True)

    parquet_count, parquet_min, parquet_max = get_parquet_stat(output_file)
    print("\nParquet statistics:", flush=True)
    print("Row count:", parquet_count, flush=True)
    print("min VALID_DTTM:", parquet_min, flush=True)
    print("max VALID_DTTM:", parquet_max, flush=True)

    # === VALIDATE AND MOVE TO FINAL FOLDER DELETE CHECKPOINT FILE ===
    if oracle_count == parquet_count and oracle_max == parquet_max and oracle_min == parquet_min:
        destination_path = "/parquet/current"
        move_parquet_file(output_file)
        # checkpoint file is no longer required for correctness; remove it to avoid confusion
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        print("SUCCESS : Conversion complete and file moved", flush=True)
    else:
        print("FAILED : Please check again", flush=True)

    # === COMPLETION LOG ===
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print(f"[INFO] Merged {len(batch_files)} files into {output_file} successfully.", flush=True)
    print(f"[INFO] Program finished at: {end_time}", flush=True)
    print(f"[INFO] Total runtime: {elapsed_time}", flush=True)

if __name__ == "__main__":
    main()