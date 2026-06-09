# Databricks notebook source
# MAGIC %md
# MAGIC # grant_sp_lakebase wrapper
# MAGIC In-workspace runner for `grant_sp_lakebase.py` — reads widget params, sets
# MAGIC them in the environment, then exec's the original script.

# COMMAND ----------

# MAGIC %pip install --upgrade "databricks-sdk>=0.40.0" psycopg2-binary requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os, runpy

dbutils.widgets.text("app_name", "", "App name")
dbutils.widgets.text("lakebase_dns", "", "Lakebase DNS")
dbutils.widgets.text("lakebase_database", "", "Lakebase database")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance (Provisioned)")
dbutils.widgets.text("lakebase_endpoint_path", "", "Lakebase endpoint path (Autoscaling)")
dbutils.widgets.text("script_path", "", "Path to grant_sp_lakebase.py in /Workspace")

os.environ["APP_NAME"] = dbutils.widgets.get("app_name")
os.environ["LAKEBASE_DNS"] = dbutils.widgets.get("lakebase_dns")
os.environ["LAKEBASE_DATABASE"] = dbutils.widgets.get("lakebase_database")
os.environ["LAKEBASE_INSTANCE"] = dbutils.widgets.get("lakebase_instance")
os.environ["LAKEBASE_ENDPOINT_PATH"] = dbutils.widgets.get("lakebase_endpoint_path")

script_path = dbutils.widgets.get("script_path")
print(f"Running grant_sp_lakebase.py from {script_path}")
print(f"  APP_NAME={os.environ['APP_NAME']}")
print(f"  LAKEBASE_DNS={os.environ['LAKEBASE_DNS']}")
print(f"  LAKEBASE_INSTANCE={os.environ['LAKEBASE_INSTANCE']}")

try:
    runpy.run_path(script_path, run_name="__main__")
    print("grant_sp_lakebase: completed without raising")
except SystemExit as e:
    code = e.code if e.code is not None else 0
    print(f"grant_sp_lakebase: exited with code {code}")
    if code != 0:
        raise RuntimeError(f"grant_sp_lakebase.py exited with non-zero code {code}")
