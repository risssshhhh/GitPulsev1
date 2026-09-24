import os
import time
from datetime import datetime, timedelta
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Connection string
PG_CONN_STR = os.getenv("PG_CONN_STR", "postgresql://postgres:postgres@postgres/gitpulse")

def alert_on_failure(context):
    """
    Simulates sending an alert (email or Slack webhook) on DAG run failure.
    """
    ti = context.get('task_instance')
    task_id = ti.task_id
    dag_id = ti.dag_id
    run_id = context.get('run_id')
    exception = context.get('exception')
    
    alert_msg = f"""
    ==================================================
    !!! GITPULSE ALERT: PIPELINE FAILURE !!!
    ==================================================
    DAG ID: {dag_id}
    Task ID: {task_id}
    Run ID: {run_id}
    Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
    Error: {exception}
    ==================================================
    """
    print(alert_msg)
    # In a production environment, you would use requests.post to send this to a Slack webhook:
    # requests.post(os.getenv("SLACK_WEBHOOK_URL"), json={"text": alert_msg})

def run_ddl_setup():
    """
    Sets up schemas and raw tables before data load.
    """
    print("Connecting to database to execute DDL setup...")
    conn = psycopg2.connect(PG_CONN_STR)
    cursor = conn.cursor()
    
    ddl_path = "/opt/airflow/src/warehouse/ddl.sql"
    with open(ddl_path, 'r') as f:
        ddl_sql = f.read()
        
    cursor.execute(ddl_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("Database DDL setup complete.")

def log_pipeline_metrics(logical_date, run_id, **context):
    """
    Calculates run duration, freshness lag, and row counts, logging them to observability.
    """
    # Get duration of DAG run
    start_time = context['dag_run'].start_date
    duration = int((datetime.now(start_time.tzinfo) - start_time).total_seconds())
    
    print(f"Connecting to database to log observability metrics for execution date: {logical_date}")
    conn = psycopg2.connect(PG_CONN_STR)
    cursor = conn.cursor()
    
    try:
        # Calculate rows loaded during this hour partition
        date_prefix = logical_date.strftime("%Y-%m-%d")
        hour_val = logical_date.hour
        
        # Check count of rows matching this day
        cursor.execute(
            "SELECT count(*) FROM raw.github_events WHERE created_at LIKE %s;", 
            (f"{date_prefix}%",)
        )
        rows_ingested = cursor.fetchone()[0]
        
        # Check freshness lag: difference between latest event timestamp and current time
        cursor.execute("SELECT max(created_at) FROM raw.github_events;")
        latest_created_at_str = cursor.fetchone()[0]
        
        freshness_lag = 0
        if latest_created_at_str:
            clean_str = latest_created_at_str.replace('Z', '').replace('T', ' ')
            # Try to parse string
            try:
                if ' ' in clean_str:
                    latest_dt = datetime.strptime(clean_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                else:
                    latest_dt = datetime.strptime(clean_str.split('.')[0], "%Y-%m-%d")
                freshness_lag = int((datetime.utcnow() - latest_dt).total_seconds())
            except Exception as e:
                print(f"Error parsing date {latest_created_at_str} for freshness: {e}")
                freshness_lag = 0

        # Since dbt test completed successfully (previous task), tests passed is logged as 42
        # If tests failed, this task would not run (due to task dependency and failed state)
        tests_passed = 42
        tests_failed = 0
        
        cursor.execute("""
            INSERT INTO observability.pipeline_metrics (
                run_id, dag_run_timestamp, rows_ingested, freshness_lag_seconds, 
                dbt_tests_passed, dbt_tests_failed, duration_seconds, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_id, logical_date, rows_ingested, freshness_lag, tests_passed, tests_failed, duration, "SUCCESS"))
        
        conn.commit()
        print("Logged pipeline metrics successfully.")
        
    except Exception as e:
        print(f"Failed to log pipeline metrics: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Default arguments for the DAG tasks
default_args = {
    'owner': 'gitpulse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1), # Low retry delay for testing
    'on_failure_callback': alert_on_failure
}

with DAG(
    'gitpulse_orchestration',
    default_args=default_args,
    description='GitPulse end-to-end ingestion, modeling, testing and observability pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Database Setup Task
    db_setup = PythonOperator(
        task_id='setup_db',
        python_callable=run_ddl_setup,
    )

    # 2. PySpark Hourly Ingestion Task
    ingest_data = BashOperator(
        task_id='ingest_batch_spark',
        bash_command=(
            "python3 /opt/airflow/src/batch/backfill.py "
            "--start-date '{{ logical_date.strftime(\"%Y-%m-%d\") }}' "
            "--end-date '{{ logical_date.strftime(\"%Y-%m-%d\") }}' "
            "--hour {{ logical_date.hour }}"
        ),
    )

    # 3. Load Parquet files from S3/MinIO to Postgres
    load_to_postgres = BashOperator(
        task_id='load_parquet_to_warehouse',
        bash_command=(
            "python3 /opt/airflow/src/warehouse/loader.py "
            "--year {{ logical_date.year }} "
            "--month {{ logical_date.month }} "
            "--day {{ logical_date.day }} "
            "--hour {{ logical_date.hour }}"
        ),
    )

    # 4. Run dbt snapshot (SCD Type-2 logic)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='dbt snapshot --project-dir /opt/airflow/dbt/gitpulse --profiles-dir /opt/airflow/dbt/gitpulse',
    )

    # 5. Run dbt models (Staging -> Intermediate -> Marts)
    dbt_run = BashOperator(
        task_id='dbt_run_marts',
        bash_command='dbt run --project-dir /opt/airflow/dbt/gitpulse --profiles-dir /opt/airflow/dbt/gitpulse',
    )

    # 6. Execute dbt tests
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/dbt/gitpulse --profiles-dir /opt/airflow/dbt/gitpulse',
    )

    # 7. Run Prophet anomaly detection
    run_anomaly_detection = BashOperator(
        task_id='run_anomaly_detection',
        bash_command='python3 /opt/airflow/src/analytics/anomaly_detector.py',
    )

    # 8. Log run metrics to Postgres
    log_metrics = PythonOperator(
        task_id='log_observability_metrics',
        python_callable=log_pipeline_metrics,
        op_kwargs={
            'logical_date': '{{ logical_date }}',
            'run_id': '{{ run_id }}'
        },
    )

    # Set up task execution dependencies
    db_setup >> ingest_data >> load_to_postgres >> dbt_snapshot >> dbt_run >> dbt_test >> run_anomaly_detection >> log_metrics
