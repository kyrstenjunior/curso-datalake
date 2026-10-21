from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

# Recupera as credenciais da conexão aws_default
aws_conn = BaseHook.get_connection("aws_default")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password
AWS_REGION = aws_conn.extra_dejson.get("region_name", "sa-east-1")

# Nome do bucket
S3_BUCKET = "curso-kyrsten-udemy-datalake"

default_args = {
    "owner": "kyrsten",
    "retries": 1
}

with DAG(
    dag_id="movies_silver_dag",
    default_args=default_args,
    description="Trata os dados extraidos da camada bronze para a camada silver",
    start_date=days_ago(1),
    schedule_interval=None, # Use o https://crontab.guru/ para definir o Schedule
    catchup=False,
    tags=["silver", "mongodb"]
) as dag:
    # wait_fot_other_dag = ExternalTaskSensor(
        # task_id="wait_for_other_id",
        # external_dag_id="movies_bronze_dag",    # DAG QUE SERÁ AGUARDADA PARA EXECUÇÃO
        # external_task_id=None,                  # SE QUISER ESPERAR UMA TASK ESPECÍFICA DA DAG ACIMA, COLOQUE AQUI
        # poke_interval=60,                       # VERIFICA A CADA 60 SEGUNDOS
        # timeout=3600,                           # AGUARDA 1 HORA
        # dag=dag
    # )

    transform_movies = DockerOperator(
        task_id="transform_movies_to_silver",
        image="spark-transformacao:latest",
        command="python3 /app/scripts/movies_silver.py",
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

    # wait_fot_other_dag >> transform_movies