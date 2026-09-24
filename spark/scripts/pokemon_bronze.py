# Databricks notebook source
import boto3
import json
import requests

# COMMAND ----------

def extract_all_pokemon():
    base_url = "https://pokeapi.co/api/v2/pokemon/"
    pokemon_list = []
    next_url = base_url

    while next_url:
        response = requests.get(next_url)
        if response.status_code == 200:
            data = response.json()
            pokemon_list.extend(data['results'])
            next_url = data.get('next')
        else:
            raise Exception(f"Failed to fetch Pokémon data. Status code: {response.status_code}")

    return pokemon_list

# COMMAND ----------

def save_to_s3(json_data, bucket_name, file_name):
    try:
        s3_client = boto3.client('s3')
        json_string = json.dumps(json_data, indent=4)
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=json_string)

        print(f"Arquivo {file_name} salvo com sucesso no bucket {bucket_name}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo no S3: {e}")

# COMMAND ----------

pokemons = extract_all_pokemon()

# COMMAND ----------

save_to_s3(json_data=pokemons, bucket_name="curso-kyrsten-udemy-datalake", file_name="bronze/api/pokemon/pokemons.json")