import pandas as pd
import logging
import subprocess
from pathlib import Path
from sqlalchemy import create_engine, text

# --- Config ---
table = "dm_mplus_billing"          # nama model dbt + tabel refine + tabel dmart
dbt_tag = "ref_prod"                # nama target di profiles.yml
source_schema = "refine"
target_schema = "dmart"
db_url = "postgresql+psycopg2://admin:admin@localhost:5432/dwh"

project_path = Path(__file__).resolve().parents[1] / "dbt"
profiles_path = project_path

# Logging setup
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


def copy_refine_to_dmart():
    """Copy tabel dari refine → dmart"""
    engine = create_engine(db_url)

    # 1. Load refine
    query = f"SELECT * FROM {source_schema}.{table}"
    logging.info(f"Mengambil data dari {source_schema}.{table} ...")

    df = pd.read_sql(query, engine)
    logging.info(f"Jumlah baris di refine: {len(df):,}")

    # 2. Write ke dmart
    logging.info(f"Menulis data ke {target_schema}.{table} ...")

    df.to_sql(
        table,
        engine,
        schema=target_schema,
        if_exists="replace",
        index=False
    )
    logging.info(f"Berhasil membuat tabel {target_schema}.{table}")
    engine.dispose()


def drop_refine_table():
    """Hapus tabel di schema refine"""
    drop_engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
    with drop_engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {source_schema}.{table} CASCADE"))
    drop_engine.dispose()
    logging.info(f"Tabel '{source_schema}.{table}' berhasil dihapus")


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