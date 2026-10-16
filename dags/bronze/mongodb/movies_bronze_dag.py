from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# Recupera as credenciais da conexão aws_default
aws_conn = BaseHook.get_connection("aws_default")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
AWS_REGION = aws_conn.extra_dejson.get("region_name", "sa-east-1")

# Recupera as credenciais da conexão mongodb_default
mongodb_conn = BaseHook.get_connection("mongo_default")
MONGODB_HOST = mongodb_conn.host
MONGODB_DB = mongodb_conn.schema
MONGODB_USER = mongodb_conn.login
MONGODB_PASSWORD = mongodb_conn.password
MONGODB_PORT = mongodb_conn.port

# Nome do bucket
S3_BUCKET = "curso-kyrsten-udemy-datalake"

default_args = {
    "owner": "kyrsten",
    "retries": 1
}

with DAG(
    dag_id="movies_bronze_dag",
    default_args=default_args,
    description="Extrai Filmes do MongoDB e salva em Parquet no S3",
    start_date=days_ago(1),
    schedule_interval=None, # Use o https://crontab.guru/ para definir o Schedule
    catchup=False,
    tags=["bronze", "mongodb"]
) as dag:
    extract_movies = DockerOperator(
        task_id="extract_movies_to_s3",
        image="spark-transformacao:latest",
        command="python3 /app/scripts/movies_bronze.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        environment={
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "MONGODB_HOST" : MONGODB_HOST,
            "MONGODB_DB" : MONGODB_DB,
            "MONGODB_USER" : MONGODB_USER,
            "MONGODB_PASSWORD" : MONGODB_PASSWORD,
            "MONGODB_PORT" : MONGODB_PORT,
            "S3_BUCKET": S3_BUCKET
        }
    )