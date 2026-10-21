import os
import io
import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.functions import col, explode
from pyspark.sql import SparkSession


def main():
    s3_client = boto3.client("s3")
    bucket = os.environ["S3_BUCKET"]
    key = "bronze/mongodb/movies/movies.parquet"
    local_path = "/tmp/movies.parquet"

    s3_client.download_file(bucket, key, local_path)
    
    df = pd.read_parquet(local_path)
    df = df[["title", "genres", "cast"]]

    def str_to_list(x):
        if isinstance(x, str) and x.strip().startswith("["):
            try:
                return json.loads(x)
            except (json.JSONDecodeError, TypeError):
                return []
        if isinstance(x, list):
            return x
        return []

    arrays_cols = ["genres", "cast"]

    for column in arrays_cols:
        df[column] = df[column].apply(str_to_list)

    df_exploded_genres = df.explode("genres").rename(columns={"genres": "genre"})
    df_exploded_cast = df_exploded_genres.explode("cast")

    df_final = df_exploded_cast[["title", "genre", "cast"]].reset_index(drop=True)
    df_final = df_final.dropna(subset=["genre", "cast"])

    silver_key = "silver/mongodb/movies/movies_silver.parquet"

    parquet_buffer = io.BytesIO()

    df_final.to_parquet(parquet_buffer, engine="pyarrow", index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
    )

    print(f"Arquivo salvo com sucesso em: s3://{bucket}/{silver_key}")

if __name__ == "__main__":
    main()