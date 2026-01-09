from airflow import DAG
from airflow.models import Variable 
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import json
import requests
import io
import pandas as pd

coins = ['bitcoin', 'ethereum', 'solana', 'cardano', 'tether']
bucket_name = "data-batch-cripto-api-raw-victorhvm-2026"

def on_failure_callback(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    error = context.get('exception')
    print(f"A tarefa '{task_id}' da DAG '{dag_id}' falhou terrivelmente, da uma olhada no erro e arruma logo: {error}")

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=Variable.get("aws_access_key"),
        aws_secret_access_key=Variable.get("aws_secret_key"),
        region_name='us-east-1'
    )

def get_crypto_bronze(coin_id):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()

    s3_client = get_s3_client()
    file_name = f"{coin_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"raw/{file_name}",
        Body=json.dumps(data)
    )

def transform_crypto_silver(coin_id):
    s3_client = get_s3_client() 

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f'raw/{coin_id}/')
    all_files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)
    
    if not all_files:
        raise Exception("Nenhum arquivo encontrado")

    latest_file = all_files[0]['Key']
    obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
    data = json.loads(obj['Body'].read().decode('utf-8'))
    
    df = pd.DataFrame([data[coin_id]])
    df['usd'] = df['usd'].astype(float)
    df['timestamp'] = datetime.now()
    df['coin'] = coin_id

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    
    target_key = f"silver/coin={coin_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=target_key,
        Body=parquet_buffer.getvalue()
    )

def generate_crypto_gold():
    s3_client = get_s3_client()
    
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='silver/')
    files = response.get('Contents', [])
    
    all_data = []
    for f in files:
        if f['Key'].endswith('.parquet'):
            obj = s3_client.get_object(Bucket=bucket_name, Key=f['Key'])
            all_data.append(pd.read_parquet(io.BytesIO(obj['Body'].read())))
    
    if not all_data:
        return

    df_full = pd.concat(all_data)
    df_gold = df_full.groupby('coin').agg({
        'usd': ['mean', 'min', 'max'],
        'timestamp': 'max'
    }).reset_index()
    
    df_gold.columns = ['coin', 'price_mean', 'price_min', 'price_max', 'last_update']

    gold_buffer = io.BytesIO()
    df_gold.to_parquet(gold_buffer, index=False)
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"gold/crypto_daily_summary.parquet",
        Body=gold_buffer.getvalue()
    )

with DAG(
    dag_id='extraction_crypto_api_to_s3',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'on_failure_callback': on_failure_callback}
) as dag:

    task_gold = PythonOperator(
        task_id='generate_gold_view',
        python_callable=generate_crypto_gold
    )

    for coin in coins:
        task_extract = PythonOperator(
            task_id=f'extract_{coin}',
            python_callable=get_crypto_bronze,
            op_args=[coin]
        )

        task_transform = PythonOperator(
            task_id=f'transform_{coin}',
            python_callable=transform_crypto_silver,
            op_args=[coin]
        )

        task_extract >> task_transform >> task_gold