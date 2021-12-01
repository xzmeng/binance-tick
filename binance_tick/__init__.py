import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from enum import Enum
from os.path import dirname, join
from typing import Optional, Union
from zipfile import ZipFile

import pandas as pd
import requests

DATA_DIR = join(dirname(dirname(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)


def create_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = create_logger()


class Kind(Enum):
    SPOT = "spot"
    FUTURES_UM = "futures/um"
    FUTURES_CM = "futures/cm"


class DataLoader:
    def __init__(
        self,
        kind: Kind,
        symbol: str,
        start: Union[date, str],
        end: Union[date, str],
        tz: str = None,
    ) -> None:
        self.kind = kind
        self.symbol = symbol
        self.start = start
        self.end = end
        self.tz = tz

    def load_data(self) -> pd.DataFrame:
        with ThreadPoolExecutor(max_workers=20) as executor:
            dfs = list(
                executor.map(self.load_daily_data, pd.date_range(self.start, self.end))
            )
        df = pd.concat(dfs)
        if self.tz:
            df.index = df.index.tz_localize("utc").tz_convert(self.tz)
        return df

    def load_daily_data(self, dt: date) -> Optional[pd.DataFrame]:
        try:
            return self.load_local_daily_data(dt)
        except FileNotFoundError:
            return self.download_daily_data(dt)

    def load_local_daily_data(self, dt: date) -> pd.DataFrame:
        pickle_path = self.get_daily_pickle_path(dt)
        return pd.read_pickle(pickle_path)

    def download_daily_data(self, dt: date) -> Optional[pd.DataFrame]:
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

    def get_daily_pickle_path(self, dt: date) -> str:
        return join(
            DATA_DIR,
            self.kind.value,
            self.symbol,
            str(dt.year),
            str(dt.month),
            f"{dt.day}.pkl",
        )


def load_data(
    symbol: str = "ETHUSDT",
    start: Union[date, str] = date(2021, 2, 28),
    end: Union[date, str] = date.today(),
    kind: Union[Kind, str] = Kind.SPOT,
    tz: str = "Asia/Shanghai",
) -> None:
    if isinstance(kind, str):
        kind = {"spot": Kind.SPOT, "cm": Kind.FUTURES_CM, "um": Kind.FUTURES_UM}[kind]
    return DataLoader(kind, symbol, start, end, tz).load_data()


if __name__ == "__main__":
    dl = DataLoader()
    df = dl.load_data()
    print(df)
