import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd

# Configurações e constantes
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")
DI_FILE = "di_data.parquet"


def get_di_on_date(date: dt.date) -> pd.DataFrame:
    df = yd.futures(contract_code="DI1", trade_date=date)
    df.drop(columns=["DaysToExp"], inplace=True)
    if "SettlementRate" not in df.columns:
        raise ValueError("There is no Settlement data for today.")
    return df


now_bz = dt.datetime.now(TIMEZONE_BZ)
today_bz = now_bz.date()
# Force a specific date for testing purposes
# today_bz = pd.to_datetime("17-12-2024", dayfirst=True)

if not yd.bday.is_business_day(today_bz):
    raise ValueError("Today is not a business day.")


(  # Load the DI data from the parquet file and update it with the new data
    pd.concat([pd.read_parquet(DI_FILE), get_di_on_date(today_bz)])
    .drop_duplicates(subset=["TradeDate", "TickerSymbol"], keep="last")
    .sort_values(["TradeDate", "ExpirationDate"])
    .reset_index(drop=True)
    .to_parquet(DI_FILE, compression="gzip", index=False)
)
