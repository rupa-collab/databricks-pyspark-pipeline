# Databricks notebook source
# COMMAND ----------
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
CIFER_BANKING_CSV_URL = "https://huggingface.co/datasets/CiferAI/Cifer-Fraud-Detection-Dataset-AF/resolve/main/Cifer-Fraud-Detection-Dataset-AF-part-1-14.csv"

# COMMAND ----------
# Create target directories

dbutils.fs.mkdirs(RETAIL_DIR)
dbutils.fs.mkdirs(BANKING_DIR)

# COMMAND ----------
# Download helper

import os
import requests


def dbfs_file_exists(path: str) -> bool:
    parent = path.rsplit("/", 1)[0]
    name = path.rsplit("/", 1)[1]
    try:
        return any(f.name == name for f in dbutils.fs.ls(parent))
    except Exception:
        return False


def download_to_dbfs(url: str, dbfs_path: str):
    if dbfs_file_exists(dbfs_path):
        print(f"Exists: {dbfs_path}")
        return

    local_tmp = f"/dbfs{dbfs_path.replace('dbfs:', '')}"
    os.makedirs(os.path.dirname(local_tmp), exist_ok=True)

    print(f"Downloading: {url} -> {dbfs_path}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

# COMMAND ----------
# Retail: download zip from UCI

try:
    download_to_dbfs(UCI_RETAIL_ZIP_URL, RETAIL_ZIP)
except Exception as exc:
    print(f"Retail download failed: {exc}")

# COMMAND ----------
# Extract retail zip and convert XLSX -> CSV

import zipfile

zip_local = f"/dbfs{RETAIL_ZIP.replace('dbfs:', '')}"
extract_dir = f"/dbfs{RETAIL_DIR.replace('dbfs:', '')}"

if os.path.exists(zip_local):
    with zipfile.ZipFile(zip_local, "r") as zf:
        zf.extractall(extract_dir)

# Convert Excel to CSV (requires pandas + openpyxl)
try:
    import pandas as pd
    xls_local = f"/dbfs{RETAIL_XLS.replace('dbfs:', '')}"
    csv_local = f"/dbfs{RETAIL_CSV.replace('dbfs:', '')}"
    if os.path.exists(xls_local) and not os.path.exists(csv_local):
        df = pd.read_excel(xls_local)
        df.to_csv(csv_local, index=False)
        print(f"Wrote {RETAIL_CSV}")
except Exception as exc:
    print("Excel -> CSV conversion failed. If pandas/openpyxl is missing, convert manually and upload CSV.")
    print(exc)

# COMMAND ----------
# Banking: download Cifer Fraud Detection CSV

try:
    download_to_dbfs(CIFER_BANKING_CSV_URL, BANKING_CSV)
except Exception as exc:
    print(f"Banking download failed: {exc}")

# COMMAND ----------
# Validation: list staged files

print("Retail files:")
for f in dbutils.fs.ls(RETAIL_DIR):
    print(f.path)

print("Banking files:")
for f in dbutils.fs.ls(BANKING_DIR):
    print(f.path)
