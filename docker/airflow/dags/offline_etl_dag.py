"""
离线数仓 ETL 调度 DAG

每天凌晨 1 点执行:
  spark-submit → ODS → DWD → DWS → ADS
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "offline_etl_dag",
    default_args=default_args,
    description="离线数仓 ETL：ODS → DWD → DWS → ADS",
    schedule="0 1 * * *",
    catchup=False,
    tags=["offline", "etl"],
)

run_etl = BashOperator(
    task_id="run_spark_etl",
    bash_command='docker exec spark-master bash -c "export PATH=/opt/spark/bin:$PATH && spark-submit --master spark://spark-master:7077 /opt/spark-etl/offline_etl.py --date {{ ds }}"',
    dag=dag,
)

# 依赖上游（如后续添加数据质量检查、数据导出等任务）
# quality_check >> data_export

if __name__ == "__main__":
    dag.test()
