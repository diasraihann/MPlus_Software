import pandas as pd
import logging
import subprocess
import psycopg2
from pathlib import Path
from sqlalchemy import create_engine, text
from io import StringIO

# --- Config ---
table = "mplus_stt"
source_schema = "staging"
target_schema = "refine"
dbt_target = "ref_prod"
db_url = "postgresql+psycopg2://admin:admin@localhost:5432/dwh"

staging_file = Path(__file__).resolve().parents[2] / f"{source_schema}" / f"{table}.parquet"
project_path = Path(__file__).resolve().parents[1] / "dbt"
profiles_path = project_path

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def copy_dataframe_to_postgres(df, table_name, schema_name):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dwh",
        user="admin",
        password="admin"
    )
    
    try:
        cursor = conn.cursor()
        # Drop table jika ada
        cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE")
        # Create table
        columns = []
        for col, dtype in df.dtypes.items():
            if dtype == 'int64':
                pg_type = 'INTEGER'
            elif dtype == 'float64':
                pg_type = 'DOUBLE PRECISION'
            elif dtype == 'bool':
                pg_type = 'BOOLEAN'
            elif dtype == 'datetime64[ns]':
                pg_type = 'TIMESTAMP'
            else:
                pg_type = 'TEXT'
            columns.append(f'"{col}" {pg_type}')
        
        create_table_sql = f"""
            CREATE TABLE {schema_name}.{table_name} (
                {', '.join(columns)}
            )
        """
        cursor.execute(create_table_sql)
        
        # Copy data
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        # Set search_path dulu baru COPY
        cursor.execute(f"SET search_path TO {schema_name}")
        cursor.copy_from(buffer, table_name, sep='\t', null='\\N')
        
        conn.commit()
        logging.info(f"Berhasil upload {len(df):,} rows ke {schema_name}.{table_name}")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def main():
    try:
        # 1. Load parquet ke Postgres
        df = pd.read_parquet(staging_file)
        copy_dataframe_to_postgres(df, table, target_schema)
        logging.info(f"Tabel '{target_schema}.{table}' berhasil dibuat dari parquet")
        
        # 2. Jalankan DBT dengan subprocess
        command = [
            "dbt", "run",
            "--select", f"tag:{table}",
            "--target", f"{dbt_target}",
            "--project-dir", str(project_path),
            "--profiles-dir", str(profiles_path)
        ]
        logging.info(f"Running DBT command: {' '.join(command)}")
        
        result = subprocess.run(command, capture_output=True, text=True, cwd=str(project_path))
        if result.stdout:
            print(result.stdout)
        if result.returncode != 0:
            logging.error("DBT run failed.")
            raise Exception("DBT run failed")
        
        logging.info("DBT run completed successfully.")
        
        # 3. Hapus tabel staging setelah DBT selesai
        drop_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
        with drop_engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {target_schema}.{table} CASCADE"))
        drop_engine.dispose()
        logging.info(f"Tabel '{target_schema}.{table}' berhasil dihapus")
        
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()