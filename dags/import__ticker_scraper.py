import os
import logging
import requests
from datetime import datetime
from bs4 import BeautifulSoup

from airflow.decorators import dag, task


DAG_NAME = "import__ticker_scraper"
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

taskflow()