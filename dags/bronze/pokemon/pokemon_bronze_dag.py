from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# Recupera as credenciais da conexão aws_default
aws_conn = BaseHook.get_connection("aws_default")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
AWS_REGION = aws_conn.extra_dejson.get("region_name", "us-east-1")

# Nome do bucket
S3_BUCKET = "curso-kyrsten-udemy-datalake"

default_args = {
    "owner": "kyrsten",
    "retries": 1
}

with DAG(
    dag_id="pokemon_bronze_dag",
    default_args=default_args,
    description="Extração de dados de Pokémons para a camada Bronze",
    start_date=days_ago(1),
    schedule_interval=None, # Use o https://crontab.guru/ para definir o Schedule
    catchup=False,
    tags=["bronze", "pokemon"]
) as dag:
    extract_pokemon = DockerOperator(
        task_id="extract_pokemon_to_s3",
        image="spark-transformacao:latest",
        command="python3 /app/scripts/pokemon_bronze.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        environment={
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
            "AWS_DEFAULT_REGION": AWS_REGION,
            "S3_BUCKET": S3_BUCKET
        }
    )