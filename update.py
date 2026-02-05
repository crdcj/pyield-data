import datetime as dt
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyield as yd

# Os arquivos estão na pasta data
base_dir = Path(__file__).parent
data_dir = base_dir / "data"
DI1_PARQUET = data_dir / "b3_di.parquet"
TPF_PARQUET = data_dir / "anbima_tpf.parquet"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_di1_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.b3.fetch_price_report(
        date=date,
        contract_code="DI1",
        source_type="PR",
    )
    selected_cols = [
        "TradeDate",
        "ExpirationDate",
        "TickerSymbol",
        "DaysToExp",
        "BDaysToExp",
        "OpenContracts",
        "TradeCount",
        "TradeVolume",
        "FinancialVolume",
        "DV01",
        "SettlementPrice",
        "MinLimitRate",
        "MaxLimitRate",
        "BestBidRate",
        "BestAskRate",
        "OpenRate",
        "MinRate",
        "AvgRate",
        "MaxRate",
        "CloseRate",
        "SettlementRate",
        "ForwardRate",
    ]
    selected_cols = [col for col in selected_cols if col in df.columns]
    return df.select(selected_cols)


def get_tpf_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.anbima.tpf_data(date=date, fetch_from_source=True)
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


@dataclass
class DatasetConfig:
    """Configuração para atualização de dataset."""

    parquet_path: Path
    fetch_function: Callable[[dt.date], pl.DataFrame]
    id_cols: list[str]
    dataset_name: str


def update_dataset(target_date: dt.date, config: DatasetConfig) -> None:
    """
    Atualiza um dataset parquet com novos dados.

    Args:
        target_date: Data dos dados a serem buscados
        config: Configuração do dataset a ser atualizado

    Raises:
        ValueError: Se não houver dados disponíveis para a data especificada
    """
    df = pl.read_parquet(config.parquet_path)
    df_new = config.fetch_function(target_date)

    if df_new.is_empty():
        raise ValueError(f"No {config.dataset_name} data available for {target_date}")

    (
        pl.concat([df, df_new], how="diagonal_relaxed")
        .unique(subset=config.id_cols, keep="last")
        .sort(config.id_cols)
        .write_parquet(config.parquet_path)
    )

    logger.info(f"{config.dataset_name} dataset updated with data from {target_date}")


# Configurações dos datasets
DI1_CONFIG = DatasetConfig(
    parquet_path=DI1_PARQUET,
    fetch_function=get_di1_on_date,
    id_cols=["TradeDate", "ExpirationDate"],
    dataset_name="DI1",
)

TPF_CONFIG = DatasetConfig(
    parquet_path=TPF_PARQUET,
    fetch_function=get_tpf_on_date,
    id_cols=["ReferenceDate", "BondType", "MaturityDate"],
    dataset_name="TPF",
)


def determine_target_date() -> dt.date:
    """
    Determina a data de referência para atualização dos dados.

    Returns:
        Data de referência apropriada baseada no dia e hora atuais
    """
    now = yd.now()
    today = now.date()

    if yd.bday.is_business_day(today):
        # É dia útil, então o horário importa
        if now.hour < 20:
            target_date = yd.bday.offset(today, -1)
        else:
            target_date = today
    else:
        # Não é dia útil, então pegamos o último que existiu
        target_date = yd.bday.last_business_day()

    return target_date


def is_special_holiday(date: dt.date) -> bool:
    """Não tem pregão no dia 24/12 e 31/12."""
    pre_xmas = dt.date(date.year, 12, 24)
    pre_ny = dt.date(date.year, 12, 31)
    return date in (pre_xmas, pre_ny)


def main() -> None:
    target_date = determine_target_date()
    logger.info(f"Determined target trade date: {target_date}")

    # Force a specific date for testing purposes
    # target_date = dt.date(2025, 12, 23)

    if is_special_holiday(target_date):
        logger.info("No trade updates on Christmas Eve or New Year's Eve.")
        return

    try:
        update_dataset(target_date, DI1_CONFIG)
        update_dataset(target_date, TPF_CONFIG)
    except Exception as e:
        logger.error(f"Failed to update datasets: {e}")
        raise


if __name__ == "__main__":
    main()
