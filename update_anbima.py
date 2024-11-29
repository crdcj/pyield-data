import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd

# Configurações e constantes
TIMEZONE_BZ = ZoneInfo("America/Sao_Paulo")
ANBIMA_FILE = "anbima_data.parquet"


def get_anbima_on_date(date: dt.date) -> pd.DataFrame:
    keep_cols = ["BondType", "ReferenceDate", "MaturityDate", "IndicativeRate"]
    return yd.anbima.data(reference_date=date)[keep_cols].copy()


now_bz = dt.datetime.now(TIMEZONE_BZ)
today_bz = now_bz.date()
# Force a specific date for testing purposes
# today_bz = pd.to_datetime("28-11-2024", dayfirst=True)

if not yd.bday.is_business_day(today_bz):
    raise ValueError("Today is not a business day.")

(  # Load the ANBIMA data from the parquet file and update it with the new data
    pd.concat([pd.read_parquet(ANBIMA_FILE), get_anbima_on_date(today_bz)])
    .drop_duplicates(subset=["ReferenceDate", "BondType", "MaturityDate"], keep="last")
    .sort_values(["ReferenceDate", "BondType", "MaturityDate"])
    .reset_index(drop=True)
    .to_parquet(ANBIMA_FILE, compression="gzip", index=False)
)
