import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd

# Configurações e constantes
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
DI_PARQUET = "di_data.parquet"
TPF_PARQUET = "anbima_data.parquet"

DI_PICKLE = "./data/b3_di.pkl.gz"
TPF_PICKLE = "./data/anbima_tpf.pkl.gz"

logger = logging.getLogger(__name__)


def get_di_on_date(date: dt.date) -> pd.DataFrame:
    df = yd.futures(contract_code="DI1", date=date)
    df.drop(columns=["DaysToExp"], inplace=True)
    if "SettlementRate" not in df.columns:
        raise ValueError("There is no Settlement data for today.")
    return df


def get_tpf_on_date(date: dt.date) -> pd.DataFrame:
    keep_cols = ["BondType", "ReferenceDate", "MaturityDate", "IndicativeRate", "Price"]
    return yd.anbima.tpf_web_data(date=date)[keep_cols].copy()


def update_di_pickle(target_date: dt.date) -> None:
    try:
        df_old = pd.read_pickle(DI_PICKLE)
        df_new = get_di_on_date(target_date)
        (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=["TradeDate", "TickerSymbol"], keep="last")
            .sort_values(["TradeDate", "ExpirationDate"])
            .reset_index(drop=True)
            .to_pickle(DI_PICKLE, compression="gzip")
        )

        logger.info(f"DI dataset updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update DI dataset: {e}")


def update_di_parquet(target_date: dt.date) -> None:
    try:
        df_old = pd.read_parquet(DI_PARQUET)
        df_new = get_di_on_date(target_date)
        (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=["TradeDate", "TickerSymbol"], keep="last")
            .sort_values(["TradeDate", "ExpirationDate"])
            .reset_index(drop=True)
            .to_parquet(DI_PARQUET, compression="gzip", index=False)
        )

        logger.info(f"DI dataset updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update DI dataset: {e}")


def update_tpf_pickle(target_date: dt.date) -> None:
    try:
        df_old = pd.read_pickle(TPF_PICKLE)
        df_new = get_tpf_on_date(target_date)
        key_cols = ["ReferenceDate", "BondType", "MaturityDate"]
        (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=key_cols, keep="last")
            .sort_values(key_cols)
            .reset_index(drop=True)
            .to_pickle(TPF_PICKLE, compression="gzip")
        )

        logger.info(f"ANBIMA dataset updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update TPF dataset: {e}")


def update_tpf_parquet(target_date: dt.date) -> None:
    try:
        df_old = pd.read_parquet(TPF_PARQUET)
        df_new = get_tpf_on_date(target_date)
        key_cols = ["ReferenceDate", "BondType", "MaturityDate"]
        (
            pd.concat([df_old, df_new])
            .drop_duplicates(subset=key_cols, keep="last")
            .sort_values(key_cols)
            .reset_index(drop=True)
            .to_parquet(TPF_PARQUET, compression="gzip", index=False)
        )

        logger.info(f"ANBIMA dataset updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update TPF dataset: {e}")


def main():
    bz_today = dt.datetime.now(BZ_TIMEZONE).date()
    target_date = bz_today - dt.timedelta(days=1)  # Yesterday

    # Force a specific date for testing purposes
    target_date = pd.to_datetime("21-03-2025", dayfirst=True).date()

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

    # update_di_parquet(target_date)
    # update_tpf_parquet(target_date)

    update_di_pickle(target_date)
    update_tpf_pickle(target_date)


if __name__ == "__main__":
    main()
