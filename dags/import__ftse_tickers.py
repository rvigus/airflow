"""
Imports FTSE 100 tickers
"""

import os
import logging
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import yfinance as yf
from pytickersymbols import PyTickerSymbols

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_NAME = "import__ftse_tickers"
logger = logging.getLogger(DAG_NAME)


@dag(
    dag_id=DAG_NAME,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    doc_md=__doc__
)


def taskflow():

    @task(task_id="get_tickers")
    def get_tickers() -> list[str]:
        pts = PyTickerSymbols()
        return pts.get_ftse_100_london_yahoo_tickers()

    @task(task_id="enrich_tickers")
    def enrich_tickers(tickers: list[str]) -> list[dict]:
        """
        Enrich ticker information concurrently with a limited number of threads.
        """
        max_workers = 3
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            result_map = executor.map(enrich_ticker, tickers)
            enriched_data = list(result_map)

        return enriched_data

    def enrich_ticker(ticker: str):
        """
        Enrich ticker information

        """
        try:
            t = yf.Ticker(ticker)
            info = t.get_info()
            keys = ['country', 'industry', 'website', 'sector', 'marketCap', 'currency', 'exchange', 'symbol', 'shortName', 'longName']
            result = {key.lower(): info.get(key) for key in keys}
            result['ticker'] = ticker

        except:
            logger.info(f"Failed for: {ticker}")
            result = {}

        return result

    @task(task_id="combine_to_dataframe")
    def combine_to_dataframe(enriched_data: list[dict]) -> pd.DataFrame:
        """
        Combine JSON results into a single dataframe
        """
        dfs = [pd.DataFrame(data, index=[0]) for data in enriched_data if data]
        combined = pd.concat(dfs, ignore_index=True)
        return combined

    @task(task_id="write_to_db")
    def write_to_db(df, table, schema, conn_id):
        """
        Write tickers to postgres
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        hook.run(sql=f"truncate {schema}.{table}", autocommit=True)

        df.to_sql(
            con=hook.get_sqlalchemy_engine(),
            name=table,
            schema=schema,
            if_exists="append",
            index=False,
        )
        logger.info(f"Written to {schema}.{table}")


    tickers = get_tickers()
    enriched_tickers = enrich_tickers(tickers)
    df = combine_to_dataframe(enriched_tickers)
    logger.info(df)
    write_to_db(df=df, table="tickers", schema="landing", conn_id="dwh")

taskflow()

