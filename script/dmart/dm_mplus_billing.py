import pandas as pd
import logging
import subprocess
import psycopg2 
from pathlib import Path
from sqlalchemy import create_engine, text
from io import StringIO

# --- Config ---
table = "dm_mplus_billing"
dbt_tag = "ref_prod"
source_schema = "refine"
target_schema = "dmart"
db_url = "postgresql+psycopg2://admin:admin@postgres:5432/dwh"

project_path = Path(__file__).resolve().parents[1] / "dbt"
profiles_path = project_path

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_dbt():
    """Menjalankan DBT model berdasarkan tag"""
    command = [
        "dbt", "run",
        "--select", f"tag:{table}",
        "--target", dbt_tag,
        "--project-dir", str(project_path),
        "--profiles-dir", str(profiles_path)
    ]
    logging.info(f"Running DBT: {' '.join(command)}")

    result = subprocess.run(command, capture_output=True, text=True, cwd=str(project_path))
    
    if result.stdout:
        print(result.stdout)
    
    if result.returncode != 0:
        logging.error("DBT run failed.")
        raise RuntimeError("DBT run failed")

    logging.info("DBT run completed successfully.")

def copy_dataframe_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        database="dwh",
        user="admin",
        password="admin"
    )
    
    try:
        cursor = conn.cursor()
        
        # 1. DROP + CREATE
        cursor.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE")

        columns = []
        for col, dtype in df.dtypes.items():
            if 'int' in str(dtype):
                pg_type = 'BIGINT'
            elif 'float' in str(dtype):
                pg_type = 'DOUBLE PRECISION'
            elif 'bool' in str(dtype):
                pg_type = 'BOOLEAN'
            elif 'datetime' in str(dtype):
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

        # 2. COPY
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)

        cursor.copy_expert(
            f"""
            COPY {schema_name}.{table_name}
            FROM STDIN 
            WITH (
                FORMAT csv,
                DELIMITER E'\\t',
                NULL '\\N'
            )
            """,
            buffer
        )

        conn.commit()
        logging.info(f"Berhasil upload {len(df):,} rows ke {schema_name}.{table_name}")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def copy_refine_to_dmart():
    """Fungsi utama untuk ETL dari refine ke dmart"""
    
    engine = create_engine(db_url)
    
    # 1. BACA DATA (dari refine)
    with engine.connect() as conn: 
        raw_conn = conn.connection
        query = f"SELECT * FROM {source_schema}.{table}"
        logging.info(f"Mengambil data dari {source_schema}.{table} ...")
        df = pd.read_sql(query, raw_conn) 
        logging.info(f"Jumlah baris di refine: {len(df):,}")

    # 2. TULIS DATA (ke dmart menggunakan Psycopg2 COPY)
    logging.info(f"Menulis data ke {target_schema}.{table} menggunakan Psycopg2 COPY...")
    copy_dataframe_to_postgres(df, table, target_schema)

def drop_refine_table():
    """Hapus tabel di schema refine (sumber data setelah load ke dmart)"""
    drop_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
    with drop_engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {source_schema}.{table} CASCADE"))
    drop_engine.dispose()
    logging.info(f"Tabel '{source_schema}.{table}' (Refine) berhasil dihapus")


def main():
    try:
        run_dbt()
        copy_refine_to_dmart()
        drop_refine_table() 

    except Exception as e:
        logging.exception(f"Script execution failed: {e}")
        raise


if __name__ == "__main__":
    main()