from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# Recupera as credenciais da conexão aws_default
aws_conn = BaseHook.get_connection("aws_default")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
AWS_REGION = aws_conn.extra_dejson.get("region_name", "sa-east-1")

# Recupera as credenciais da conexão supabase_default
supabase_conn = BaseHook.get_connection("supabase_default")
SUPABASE_HOST = supabase_conn.host
SUPABASE_PORT = str(supabase_conn.port)
SUPABASE_DB = supabase_conn.schema
SUPABASE_USER = supabase_conn.login
SUPABASE_PASSWORD = supabase_conn.password

# Nome do bucket
S3_BUCKET = "curso-kyrsten-udemy-datalake"

default_args = {
    "owner": "kyrsten",
    "retries": 1
}

with DAG(
    dag_id="pg_tables_bronze_dag",
    default_args=default_args,
    description="Extrai pg_tables do Supabase e salva em Parquet no S3",
    start_date=days_ago(1),
    schedule_interval=None, # Use o https://crontab.guru/ para definir o Schedule
    catchup=False,
    tags=["bronze", "postgres", "supabase"]
) as dag:
    extract_pg_tables = DockerOperator(
        task_id="extract_pg_tables_to_s3",
        image="spark-transformacao:latest",
        command="python3 /app/scripts/pg_tables_bronze.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        environment={
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "SUPABASE_HOST" : SUPABASE_HOST,
            "SUPABASE_PORT" : SUPABASE_PORT,
            "SUPABASE_DB" : SUPABASE_DB,
            "SUPABASE_USER" : SUPABASE_USER,
            "SUPABASE_PASSWORD" : SUPABASE_PASSWORD,
            "S3_BUCKET": S3_BUCKET
        }
    )