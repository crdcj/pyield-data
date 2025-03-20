import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd

# Configurações e constantes
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
TPF_PARQUET = "anbima_data.parquet"
TPF_PICKLE = "data/anbima_tpf.pkl.gz"
DI_PARQUET = "di_data.parquet"
DI_PICKLE = "data/b3_di.pkl.gz"

logger = logging.getLogger(__name__)


def get_di_on_date(date: dt.date) -> pd.DataFrame:
    df = yd.futures(contract_code="DI1", date=date)
    df.drop(columns=["DaysToExp"], inplace=True)
    if "SettlementRate" not in df.columns:
        raise ValueError("There is no Settlement data for today.")
    return df


def get_tpf_on_date(date: dt.date) -> pd.DataFrame:
    keep_cols = ["BondType", "ReferenceDate", "MaturityDate", "IndicativeRate", "Price"]
    return yd.anbima.tpf_data(date=date)[keep_cols].copy()


def update_di_dataset(target_date: dt.date) -> None:
    try:
        df_old = pd.read_parquet(DI_PARQUET)
        df_new = get_di_on_date(target_date)
        df_updated = (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=["TradeDate", "TickerSymbol"], keep="last")
            .sort_values(["TradeDate", "ExpirationDate"])
            .reset_index(drop=True)
        )

        # Save to parquet and pickle
        df_updated.to_parquet(DI_PARQUET, compression="gzip", index=False)
        df_updated.to_pickle(DI_PICKLE, compression="gzip")

        logger.info(f"DI dataset updated with data from {target_date}")

    except Exception as e:
        logger.error(f"Failed to update DI dataset: {e}")


def update_tpf_dataset(target_date: dt.date) -> None:
    try:
        df_old = pd.read_parquet(TPF_PARQUET)
        df_new = get_tpf_on_date(target_date)
        key_cols = ["ReferenceDate", "BondType", "MaturityDate"]
        df_updated = (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=key_cols, keep="last")
            .sort_values(key_cols)
            .reset_index(drop=True)
        )

        # Save to parquet and pickle
        df_updated.to_parquet(TPF_PARQUET, compression="gzip", index=False)
        df_updated.to_pickle(TPF_PICKLE, compression="gzip")

        logger.info(f"ANBIMA dataset updated with data from {target_date}")

    except Exception as e:
        logger.error(f"Failed to update TPF dataset: {e}")


def main():
    bz_today = dt.datetime.now(BZ_TIMEZONE).date()
    target_date = bz_today - dt.timedelta(days=1)  # Yesterday

    # Force a specific date for testing purposes
    # target_date = pd.to_datetime("30-12-2024", dayfirst=True).date()

    if not yd.bday.is_business_day(target_date):
        logger.warning("Target date is not a business day. Aborting...")
        return

    pre_xmas = dt.date(target_date.year, 12, 24)
    pre_ny = dt.date(target_date.year, 12, 31)
    if target_date == pre_xmas or target_date == pre_ny:
        logger.warning(
            "There is no session on the day before Christmas or New Year's Eve. Aborting..."
        )
        return

    update_di_dataset(target_date)
    update_tpf_dataset(target_date)


if __name__ == "__main__":
    main()
