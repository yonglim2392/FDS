from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# 프로젝트 루트 경로를 시스템 패스에 추가하여 배치 잡 임포트 허용
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from fds_batch_job import run_batch_feature_engineering

default_args = {
    'owner': 'yong_data_eng',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fds_user_profile_batch_daily',
    default_args=default_args,
    description='FDS 데이터 레이크로부터 30일 장기 배치 피처를 연산하여 레디스에 동기화함',
    schedule_interval='@daily', # 매일 자정 실행
    catchup=False,
) as dag:

    # PythonOperator를 이용해 전날 적재된 PostgreSQL 데이터를 요약 집계 가공함
    calculate_profile_features = PythonOperator(
        task_id='calculate_and_sync_30d_features',
        python_callable=run_batch_feature_engineering
    )

    calculate_profile_features
