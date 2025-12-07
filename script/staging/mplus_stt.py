import os
import gdown
import pandas as pd
import glob
import shutil

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))      
SCRIPT_ROOT = os.path.dirname(SCRIPT_DIR)                   

# 2. TEMP dan output STAGING parquet
TEMP_DIR = os.path.join(SCRIPT_ROOT, "temp_files")           # .../script/temp_files
OUTPUT_PARQUET = os.path.join(os.path.dirname(SCRIPT_ROOT), "staging", "mplus_stt.parquet")

os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(os.path.dirname(OUTPUT_PARQUET), exist_ok=True)

# 3. Google Drive folder ID
FOLDER_ID = "1hsdwEaMmqSlMapOyixbL5qyWm-yAwTVb"

def download_all_files():
    print("Downloading all files inside Google Drive folder...")

    gdown.download_folder(
        id=FOLDER_ID,
        output=TEMP_DIR,
        quiet=False,
        use_cookies=False
    )

    print(f"All files downloaded to: {TEMP_DIR}")

# 5. UNION semua file CSV/Excel
def union_and_save():
    files = glob.glob(os.path.join(TEMP_DIR, "*"))
    print("Found files:", files)

    dfs = []

    for file in files:
        fname = os.path.basename(file)
        print(f"Reading {fname} ...")

        if fname.lower().endswith(".csv"):
            df = pd.read_csv(file)
        elif fname.lower().endswith((".xls", ".xlsx")):
            df = pd.read_excel(file)
        else:
            print(f"Skipping {fname} (unsupported format)")
            continue

        df["source_file"] = fname
        dfs.append(df)

    if not dfs:
        print("No valid files found!")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_parquet(OUTPUT_PARQUET, index=False)

    print(f"\nSaved unified parquet → {OUTPUT_PARQUET}")

# 6. Hapus folder TEMP
def cleanup_temp():
    if os.path.exists(TEMP_DIR):
        print("\nRemoving temp folder completely...")
        shutil.rmtree(TEMP_DIR)
        print("Temp folder removed.")

# 7. RUN
if __name__ == "__main__":
    download_all_files()
    union_and_save()
    cleanup_temp()