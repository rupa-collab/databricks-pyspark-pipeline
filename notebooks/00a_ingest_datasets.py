# Databricks notebook source
# Dataset ingestion helpers (Databricks)
# Downloads and stages public retail + banking datasets into Unity Catalog Volumes.

# COMMAND ----------

# Target base path provided by user

TARGET_BASE = "dbfs:/Volumes/workspace/default/datasets"
# Derived paths
RETAIL_DIR = f"{TARGET_BASE}/online_retail"
BANKING_DIR = f"{TARGET_BASE}/cifer_fraud"

# File names
RETAIL_ZIP = f"{RETAIL_DIR}/online_retail.zip"
RETAIL_XLS = f"{RETAIL_DIR}/Online Retail.xlsx"
RETAIL_CSV = f"{RETAIL_DIR}/online_retail.csv"

BANKING_CSV = f"{BANKING_DIR}/Cifer-Fraud-Detection-Dataset-AF-part-1-14.csv"

# COMMAND ----------

# Source URLs (public)
UCI_RETAIL_ZIP_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
RETAIL_CSV_URL = ""  # Optional direct CSV URL if you have one
CIFER_BANKING_CSV_URL = "https://huggingface.co/datasets/CiferAI/Cifer-Fraud-Detection-Dataset-AF/resolve/main/Cifer-Fraud-Detection-Dataset-AF-part-1-14.csv"

# Some clusters forbid local filesystem access. Keep this off unless allowed.
ALLOW_LOCAL_FS = False

# COMMAND ----------

# Create target directories

dbutils.fs.mkdirs(RETAIL_DIR)
dbutils.fs.mkdirs(BANKING_DIR)

# COMMAND ----------

# Download helper

import os


def dbfs_file_exists(path: str) -> bool:
    parent = path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[1]
    try:
        return any(f.name == name for f in dbutils.fs.ls(parent))
    except Exception:
        return False


def download_to_dbfs(url: str, dbfs_path: str):
    if dbfs_file_exists(dbfs_path):
        display(f"Exists: {dbfs_path}")
        return

    display(f"Downloading: {url} -> {dbfs_path}")
    dbutils.fs.cp(url, dbfs_path, True)

# COMMAND ----------

# Retail: prefer direct CSV if available; otherwise download zip only.
 
try:
    if RETAIL_CSV_URL:
        download_to_dbfs(RETAIL_CSV_URL, RETAIL_CSV)
    else:
        download_to_dbfs(UCI_RETAIL_ZIP_URL, RETAIL_ZIP)
        display("Retail ZIP downloaded. If your cluster forbids local filesystem access,")
        display("upload a CSV export manually or set RETAIL_CSV_URL to a direct CSV URL.")
except Exception as exc:
    display(f"Retail download failed: {exc}")


# COMMAND ----------

# Optional: extract retail zip and convert XLSX -> CSV (requires local FS access)
# Optional: extract retail zip and convert XLSX -> CSV
# If local filesystem access is disabled, try spark-excel; otherwise upload CSV manually.

def dbfs_exists(path: str) -> bool:
    parent = path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[1]
    try:
        return any(f.name == name for f in dbutils.fs.ls(parent))
    except Exception:
        return False


if ALLOW_LOCAL_FS:
    import zipfile
    import pandas as pd
    zip_local = f"/dbfs{RETAIL_ZIP.replace('dbfs:', '')}"
    extract_dir = f"/dbfs{RETAIL_DIR.replace('dbfs:', '')}"
    if os.path.exists(zip_local):
        with zipfile.ZipFile(zip_local, "r") as zf:
            zf.extractall(extract_dir)
    xls_local = f"/dbfs{RETAIL_XLS.replace('dbfs:', '')}"
    csv_local = f"/dbfs{RETAIL_CSV.replace('dbfs:', '')}"
    if os.path.exists(xls_local):
        df = pd.read_excel(xls_local)
        df.to_csv(csv_local, index=False)
        display(f"Wrote {RETAIL_CSV}")
else:
    # Try spark-excel if the XLSX is present in DBFS/Volumes
    if dbfs_exists(RETAIL_XLS) and not dbfs_exists(RETAIL_CSV):
        try:
            df = (spark.read.format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(RETAIL_XLS)
            )
            tmp_dir = f"{RETAIL_DIR}/_tmp_csv"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)
            part_file = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".csv")][0]
            dbutils.fs.cp(part_file, RETAIL_CSV, True)
            dbutils.fs.rm(tmp_dir, True)
            display(f"Wrote {RETAIL_CSV} using spark-excel.")
        except Exception as exc:
            display("Could not convert XLSX to CSV. If spark-excel is not installed, upload CSV manually.")
            display(exc)

# COMMAND ----------

# Banking: download Cifer Fraud Detection CSV

try:
    download_to_dbfs(CIFER_BANKING_CSV_URL, BANKING_CSV)
except Exception as exc:
    display(f"Banking download failed: {exc}")

# COMMAND ----------

display(dbutils.fs.ls(RETAIL_DIR))
display(dbutils.fs.ls(BANKING_DIR))
