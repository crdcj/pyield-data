import datetime as dt
import logging
from pathlib import Path

import polars as pl
import pyield as yd

# Os arquivos estão na pasta data
base_dir = Path(__file__).parent
data_dir = base_dir / "data"

DI1_PARQUET = data_dir / "b3_di.parquet"
TPF_PARQUET = data_dir / "anbima_tpf.parquet"


logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_di1_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.b3.fetch_price_report(
        date=date,
        contract_code="DI1",
        source_type="PR",
    )
    if df.is_empty():
        raise ValueError("There is no DI1 data for today.")
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
    df = pl.read_parquet(DI1_PARQUET)
    df_new = get_di1_on_date(target_date)
    (
        pl.concat([df, df_new], how="diagonal_relaxed")
        .unique(subset=["TradeDate", "TickerSymbol"], keep="last")
        .sort("TradeDate", "ExpirationDate")
        .write_parquet(DI1_PARQUET, compression="gzip")
    )
    logger.info(f"DI dataset updated with data from {target_date}")


def update_tpf_dataset(target_date: dt.date) -> None:
    df = pl.read_parquet(TPF_PARQUET)
    df_new = get_tpf_on_date(target_date)

    key_cols = ["ReferenceDate", "BondType", "MaturityDate"]
    (
        pl.concat([df, df_new], how="diagonal_relaxed")
        .unique(subset=key_cols, keep="last")
        .sort(key_cols)
        .write_parquet(TPF_PARQUET, compression="gzip")
    )
    logger.info(f"TPF parquet updated with data from {target_date}")


def main():
    now = yd.now()
    today = now.date()

    # Usando is_business_day torna a intenção mais explícita
    if yd.bday.is_business_day(today):
        # É dia útil, então o horário importa
        if now.hour < 21:
            target_date = yd.bday.offset(today, -1)
        else:
            target_date = today
    else:
        # Não é dia útil, então pegamos o último que existiu
        target_date = yd.bday.last_business_day()

    logger.info(f"Determined target trade date: {target_date}")
    # Force a specific date for testing purposes
    # target_date = dt.date.strptime("23-12-2025", "%d-%m-%Y")

    pre_xmas = dt.date(target_date.year, 12, 24)
    pre_ny = dt.date(target_date.year, 12, 31)
    if target_date in (pre_xmas, pre_ny):
        logger.info("No trade updates on Christmas Eve or New Year's Eve.")
        exit(0)

    try:
        update_di1_dataset(target_date)
        update_tpf_dataset(target_date)
    except Exception as e:
        logger.error(f"Failed to update datasets: {e}")
        raise


if __name__ == "__main__":
    main()
