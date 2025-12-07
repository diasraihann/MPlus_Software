import pandas as pd
import logging
import subprocess
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

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

def main():
    engine = None
    try:
        # 1. Load parquet ke Postgres
        engine = create_engine(db_url)
        df = pd.read_parquet(staging_file)
        df.to_sql(table, engine, schema=target_schema, if_exists="replace", index=False)
        logging.info(f"Tabel '{target_schema}.{table}' berhasil dibuat dari parquet")
        engine.dispose()
        engine = None
        
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
    finally:
        if engine is not None:
            engine.dispose()

if __name__ == "__main__":
    main()