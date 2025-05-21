from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from datetime import datetime
import requests
import os

# Configuration
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
API_KEY = os.getenv('FINNHUB_API_KEY', 'demo')  # Use .env or secrets backend
POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1)
}

with DAG(
    dag_id='stock_etl_pipeline',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
    tags=['stocks']
) as dag:

    @task()
    def extract_stock_data():
        """Extract stock data for multiple symbols."""
        all_data = []
        for symbol in STOCK_SYMBOLS:
            url = f'https://finnhub.io/api/v1/quote?symbol={symbol}&token={API_KEY}'
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                data['symbol'] = symbol
                all_data.append(data)
            else:
                raise Exception(f"Failed to fetch stock data for {symbol}: {response.status_code}")
        return all_data

    @task()
    def transform_stock_data(data_list: list):
        """Transform a list of stock data dicts."""
        transformed = []
        for data in data_list:
            transformed.append({
                'symbol': data['symbol'],
                'current_price': data['c'],
                'high_price': data['h'],
                'low_price': data['l'],
                'open_price': data['o'],
                'previous_close': data['pc'],
                'quote_time': datetime.fromtimestamp(data['t']).isoformat()
            })
        return transformed

    @task()
    def load_to_postgres(stocks: list):
        """Load multiple stock rows into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_quotes (
            id SERIAL PRIMARY KEY,
            symbol TEXT,
            current_price NUMERIC,
            high_price NUMERIC,
            low_price NUMERIC,
            open_price NUMERIC,
            previous_close NUMERIC,
            quote_time TIMESTAMP
        );
        """)

        for stock in stocks:
            cursor.execute("""
            INSERT INTO stock_quotes (
                symbol, current_price, high_price, low_price,
                open_price, previous_close, quote_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                stock['symbol'],
                stock['current_price'],
                stock['high_price'],
                stock['low_price'],
                stock['open_price'],
                stock['previous_close'],
                stock['quote_time']
            ))

        conn.commit()
        cursor.close()

    # DAG Flow
    raw_data = extract_stock_data()
    transformed = transform_stock_data(raw_data)
    load_to_postgres(transformed)
