from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def extract_cdc():
    hook = MsSqlHook(mssql_conn_id="mssql_cdc")
    sql = """
    DECLARE @from_lsn VARBINARY(10) =
      (SELECT last_lsn FROM etl_state WHERE job_name='orders_cdc');

    DECLARE @to_lsn VARBINARY(10) = sys.fn_cdc_get_max_lsn();

    SELECT *
    FROM cdc.fn_cdc_get_all_changes_dbo_orders(@from_lsn, @to_lsn, 'all');
    """
    rows = hook.get_records(sql)
    # 👉 rows 적재 로직 (S3 / BigQuery / Redshift / RDS 등)
