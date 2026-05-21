from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# sys.path.append 안티패턴 제거. 
# 실무 프로덕션 환경의 Airflow에서는 디렉터리 배포 규칙에 맞춰 PYTHONPATH 환경 변수를 다루거나
# plugins 폴더 또는 dags 컨텍스트 내 포함 경로로 의존성을 배포함.
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
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Airflow 템플릿 콘텍스트의 데이터 논리적 기준 시각인 {{ ts }} 파라미터를 넘겨 멱등 연산 체계 완성
    calculate_profile_features = PythonOperator(
        task_id='calculate_and_sync_30d_features',
        python_callable=run_batch_feature_engineering,
        op_kwargs={'base_date': '{{ ts }}'}
    )

    calculate_profile_features
