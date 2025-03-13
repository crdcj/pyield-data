import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd

# Configurações e constantes
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
ANBIMA_FILE = "anbima_data.parquet"
DI_FILE = "di_data.parquet"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_di_on_date(date: dt.date) -> pd.DataFrame:
    df = yd.futures(contract_code="DI1", date=date)
    df.drop(columns=["DaysToExp"], inplace=True)
    if "SettlementRate" not in df.columns:
        raise ValueError("There is no Settlement data for today.")
    return df


def get_anbima_on_date(date: dt.date) -> pd.DataFrame:
    keep_cols = ["BondType", "ReferenceDate", "MaturityDate", "IndicativeRate"]
    return yd.anbima.tpf_data(date=date)[keep_cols].copy()


def main():
    bz_today = dt.datetime.now(BZ_TIMEZONE).date()
    target_date = bz_today - dt.timedelta(days=1)  # Yesterday

    # Force a specific date for testing purposes
    # target_date = pd.to_datetime("30-12-2024", dayfirst=True).date()

    if not yd.bday.is_business_day(target_date):
        logging.warning("Target date is not a business day. Aborting...")
        return

    pre_xmas = dt.date(target_date.year, 12, 24)
    pre_ny = dt.date(target_date.year, 12, 31)
    if target_date == pre_xmas or target_date == pre_ny:
        logging.warning(
            "There is no session on the day before Christmas or New Year's Eve. Aborting..."
        )
        return

    try:
        (  # Load the DI data from the parquet file and update it with the new data
            pd.concat([pd.read_parquet(DI_FILE), get_di_on_date(target_date)])
            .drop_duplicates(subset=["TradeDate", "TickerSymbol"], keep="last")
            .sort_values(["TradeDate", "ExpirationDate"])
            .reset_index(drop=True)
            .to_parquet(DI_FILE, compression="gzip", index=False)
        )

    except Exception as e:
        logging.error(f"Failed to update DI dataset: {e}")

    try:
        subset_cols = ["ReferenceDate", "BondType", "MaturityDate"]
        (  # Load the ANBIMA data from the parquet file and update it with the new data
            pd.concat([pd.read_parquet(ANBIMA_FILE), get_anbima_on_date(target_date)])
            .drop_duplicates(subset=subset_cols, keep="last")
            .sort_values(subset_cols)
            .reset_index(drop=True)
            .to_parquet(ANBIMA_FILE, compression="gzip", index=False)
        )

    except Exception as e:
        logging.error(f"Failed to update ANBIMA dataset: {e}")


if __name__ == "__main__":
    main()
