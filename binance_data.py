import os
import io
import logging
from datetime import date
from enum import Enum
from os.path import dirname, join
from zipfile import ZipFile
from typing import Union
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests

DATA_DIR = join(dirname(dirname(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)


class Kind(Enum):
    SPOT = "spot"
    FUTURES_UM = "futures/um"
    FUTURES_CM = "futures/cm"


class DataLoader:
    def __init__(
        self,
        kind: Kind = Kind.SPOT,
        symbol: str = "ETHUSDT",
        start: Union[date, str] = date(2021, 2, 28),
        end: Union[date, str] = date.today(),
    ) -> None:
        self.kind = kind
        self.symbol = symbol
        self.start = start
        self.end = end

    def load_data(self):
        with ThreadPoolExecutor(max_workers=20) as executor:
            dfs = list(
                executor.map(self.load_daily_data, pd.date_range(self.start, self.end))
            )
        return pd.concat(dfs)

    def load_daily_data(self, dt: date) -> pd.DataFrame:
        try:
            return self.load_local_daily_data(dt)
        except FileNotFoundError:
            return self.download_daily_data(dt)

    def load_local_daily_data(self, dt: date) -> pd.DataFrame:
        pickle_path = self.get_daily_pickle_path(dt)
        return pd.read_pickle(pickle_path)

    def download_daily_data(self, dt: date):
        logger.info(f'Downloading {self.symbol} {dt.strftime("%Y-%m-%d")}')
        url = f'https://data.binance.vision/data/{self.kind.value}/daily/aggTrades/{self.symbol}/{self.symbol}-aggTrades-{dt.strftime("%Y-%m-%d")}.zip'
        resp = requests.get(url)
        if resp.status_code == 404:
            logger.warning(f"404: {url}")
            # pd.concat() will silently drop None object
            return None
        if resp.status_code != 200:
            raise RuntimeError(resp.reason)
        with ZipFile(io.BytesIO(resp.content)) as zf:
            with zf.open(zf.namelist()[0]) as f:
                df = pd.read_csv(f, usecols=[1, 5], names=["price", "datetime"])
        df["datetime"] = pd.to_datetime(df.datetime, unit="ms")
        df.set_index("datetime", inplace=True)
        df = df.resample("1s").first().dropna()
        pkl_path = self.get_daily_pickle_path(dt)
        os.makedirs(dirname(pkl_path), exist_ok=True)
        df.to_pickle(pkl_path)
        return df

    def csv_to_pickle(self, csv_path, pkl_path) -> None:
        df = pd.read_csv(csv_path, usecols=[1, 5], names=["price", "datetime"])

    def get_daily_pickle_path(self, dt: date) -> str:
        return join(
            DATA_DIR,
            self.kind.value,
            self.symbol,
            str(dt.year),
            str(dt.month),
            f"{dt.day}.pkl",
        )


if __name__ == "__main__":
    dl = DataLoader()
    df = dl.load_data()
    print(df)
