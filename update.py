import datetime as dt
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pyield as yd

# Configurações e constantes
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")

# Os arquivos estão na pasta data
base_dir = Path(__file__).parent
data_dir = base_dir / "data"

DI_PARQUET = data_dir / "b3_di.parquet"
TPF_PARQUET = data_dir / "anbima_tpf.parquet"


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_di1_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.futures(contract_code="DI1", date=date).drop("DaysToExp")
    if df.is_empty():
        raise ValueError("There is no DI1 data for today.")
    if "SettlementRate" not in df.columns:
        raise ValueError("There is no Settlement data for today.")
    return df


def get_tpf_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.anbima.tpf_data(date=date, fetch_from_source=True)
    if df.is_empty():
        raise ValueError("There is no TPF data for today.")

    selected_cols = [
        "BondType",
        "ReferenceDate",
        "SelicCode",
        "IssueBaseDate",
        "MaturityDate",
        "BDToMat",
        "Duration",
        "DV01",
        "DV01USD",
        "Price",
        "BidRate",
        "AskRate",
        "IndicativeRate",
        "DIRate",
    ]
    selected_cols = [col for col in selected_cols if col in df.columns]
    return df.select(selected_cols)


def update_di1_dataset(target_date: dt.date) -> None:
    try:
        df = pl.read_parquet(DI_PARQUET)
        df_new = get_di1_on_date(target_date)
        (
            pl.concat([df, df_new], how="diagonal")
            .unique(subset=["TradeDate", "TickerSymbol"], keep="last")
            .sort(["TradeDate", "ExpirationDate"])
            .write_parquet(DI_PARQUET, compression="gzip")
        )

        logger.info(f"DI dataset updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update DI dataset: {e}")


def update_tpf_dataset(target_date: dt.date) -> None:
    try:
        df = pl.read_parquet(TPF_PARQUET)
        df_new = get_tpf_on_date(target_date)

        key_cols = ["ReferenceDate", "BondType", "MaturityDate"]
        (
            pl.concat([df, df_new], how="diagonal")
            .unique(subset=key_cols, keep="last")
            .sort(key_cols)
            .write_parquet(TPF_PARQUET, compression="gzip")
        )

        logger.info(f"TPF parquet updated with data from {target_date}")
    except Exception as e:
        logger.error(f"Failed to update TPF parquet: {e}")


def main():
    target_date = yd.bday.last_business_day()
    # Force a specific date for testing purposes
    # target_date = dt.datetime.strptime("24-10-2025", "%d-%m-%Y").date()

    pre_xmas = dt.date(target_date.year, 12, 24)
    pre_ny = dt.date(target_date.year, 12, 31)
    if target_date == pre_xmas or target_date == pre_ny:
        logger.warning(
            "There is no session on the day before Christmas or New Year's Eve. Aborting..."
        )
        return

    update_di1_dataset(target_date)
    update_tpf_dataset(target_date)


if __name__ == "__main__":
    main()
