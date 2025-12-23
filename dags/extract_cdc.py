from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
import io

def extract_cdc():
    hook = MsSqlHook(mssql_conn_id="mssql_cdc")
    sql = """
    DECLARE @from_lsn VARBINARY(10) =
      (SELECT last_lsn FROM etl_state WHERE job_name='ItemLog_cdc');
      IF @from_lsn is null
		 SET @from_lsn = sys.fn_cdc_get_min_lsn('dbo_ItemLog');

    DECLARE @to_lsn VARBINARY(10) = sys.fn_cdc_get_max_lsn();

    SELECT *
    FROM cdc.fn_cdc_get_all_changes_dbo_ItemLog(@from_lsn, @to_lsn, 'all');
    """
    rows = mssql.get_pandas_df(sql)

    if rows.empty:
        return

    # Parquet 변환
    buffer = io.BytesIO()
    rows.to_parquet(buffer, index=False)

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_bytes(
        bytes_data=buffer.getvalue(),
        key="cdc/orders/dt={{ ds }}/orders.parquet",
        bucket_name="my-data-lake",
        replace=True
    )

dag = DAG(
    dag_id="sqlserver_cdc_to_s3",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False
)