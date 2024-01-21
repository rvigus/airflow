"""
Imports FTSE 100 tickers
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_NAME = "import__ftse_tickers"
logger = logging.getLogger(DAG_NAME)

dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)
dag.doc_md = __doc__

def get_tickers():
    """
    Scrape available tickers on the FTSE100
    """

    logger.info("Fetching tickers")
    url = "https://www.hl.co.uk/shares/stock-market-summary/ftse-100"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    rows = soup.find_all(id=lambda x: x and x.startswith("ls-row-"))

    tickers = []

    for row in rows:
        ticker = row.findNext('td').text
        tickers.append(ticker)

    logger.info(f"Tickers: {tickers}")

    return tickers

def write_to_db(tickers, table, schema, conn_id):
    """
    Write tickers to postgres

    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    df = pd.DataFrame(tickers, columns=['Ticker'])
    df.to_sql(
        con=hook.get_sqlalchemy_engine(),
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    logger.info(f"Written to {schema}.{table}")


def taskflow(table, schema, conn_id):
    tickers = get_tickers()
    write_to_db(tickers, table, schema, conn_id)


import_tickers = PythonOperator(
    python_callable=taskflow,
    op_kwargs={"table": "tickers", "schema": "public", "conn_id": "dwh"},
    task_id="import_tickers",
    dag=dag,
)


