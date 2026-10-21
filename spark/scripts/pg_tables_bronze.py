import os
import io
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text, URL

def main():
    # Credenciais do Supabase (vêm das env vars passadas pelo DockerOperator)
    host = os.environ["SUPABASE_HOST"]
    port = os.environ["SUPABASE_PORT"]
    database = os.environ["SUPABASE_DB"]
    user = os.environ["SUPABASE_USER"]
    password = os.environ["SUPABASE_PASSWORD"]
    bucket = os.environ["S3_BUCKET"]

    # Conexão com o Supabase (pg8000 negocia SSL automaticamente)
    # URL.create() escapa automaticamente caracteres especiais (@, :, /) na senha
    url = URL.create(
        drivername="postgresql+pg8000",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database,
    )
    engine = create_engine(url)

    # Lê a tabela pg_tables para um DataFrame pandas
    with engine.connect() as conn:
        df = pd.read_sql(text('SELECT * FROM "Pokemons"'), conn)

    print(f"Lidas {len(df)} linhas de Pokemons")

    # Converte para parquet em memória
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    # Upload para o S3
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key="bronze/postgres/Pokemon/pokemon.parquet",
        Body=buffer.getvalue(),
    )

    print(f"Arquivo salvo em s3://{bucket}/bronze/postgres/Pokemon/pokemon.parquet")

if __name__ == "__main__":
    main()