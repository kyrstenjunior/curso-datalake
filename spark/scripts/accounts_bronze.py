import os
import io
import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from urllib.parse import quote_plus


def main():
    user = os.environ["MONGODB_USER"]
    password = os.environ["MONGODB_PASSWORD"]
    host = os.environ["MONGODB_HOST"]
    bucket = os.environ["S3_BUCKET"]

    user_enc = quote_plus(user)
    pass_enc = quote_plus(password)

    uri = f"mongodb+srv://{user_enc}:{pass_enc}@mongodb-curso.nzwic7s.mongodb.net/?appName=mongodb-curso"

    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    print(f"Conectado ao MongoDB: {host}")

    db = client["sample_mflix"]
    col = db["users"]
    docs = list(col.find({}, {"_id": 0}))
    client.close()

    print(f"Lidas {len(docs)} linhas da collection '{col}'")

    df = pd.DataFrame(docs)

    for col_name in df.columns:
        if df[col_name].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col_name] = df[col_name].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else x
            )

    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=f"bronze/mongodb/accounts/accounts.parquet",
        Body=buffer.getvalue(),
    )

    print(f"Arquivo salvo em s3://{bucket}/bronze/mongodb/accounts/accounts.parquet")


if __name__ == "__main__":
    main()