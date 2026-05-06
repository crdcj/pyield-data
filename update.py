import datetime as dt
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyield as yd
from pyield.b3.boletim import buscar as boletim_buscar

# Artefatos locais do workflow: o job baixa do latest release para
# `release_staging/`, atualiza em memoria e depois publica novamente no release.
BASE_DIR = Path(__file__).parent
RELEASE_DATA_DIR = BASE_DIR / "release_staging"
TPF_PARQUET = RELEASE_DATA_DIR / "anbima_tpf.parquet"
FUTURES_PARQUET = RELEASE_DATA_DIR / "b3_futures.parquet"
FUTURES_TICKERS = ["DI1", "DDI", "FRC", "FRO", "DAP", "DOL", "WDO", "IND", "WIN"]

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class DatasetConfig:
    """Configuração para atualização de dataset."""

    parquet_path: Path
    fetch_function: Callable[[dt.date], pl.DataFrame]
    id_cols: list[str]
    dataset_name: str


def get_futures_on_date(date: dt.date) -> pl.DataFrame:
    df = boletim_buscar(
        data=date,
        prefixo_ticker=FUTURES_TICKERS,
        comprimento_ticker=6,
        boletim_completo=True,
    )
    logger.info(f"B3 boletim_buscar({date}): shape={df.shape}")
    if df.is_empty():
        raise ValueError(f"No futures data available for {date}")
    return df


def get_tpf_on_date(date: dt.date) -> pl.DataFrame:
    df = yd.tpf.taxas(data=date, completo=True)
    if df.is_empty():
        raise ValueError(f"No TPF data available for {date}")
    return df


# Configurações dos datasets
TPF_CONFIG = DatasetConfig(
    parquet_path=TPF_PARQUET,
    fetch_function=get_tpf_on_date,
    id_cols=["data_referencia", "titulo", "data_vencimento"],
    dataset_name="TPF",
)

FUTURES_CONFIG = DatasetConfig(
    parquet_path=FUTURES_PARQUET,
    fetch_function=get_futures_on_date,
    id_cols=["TradDt", "TckrSymb"],
    dataset_name="B3 Futures",
)


def determine_target_date() -> dt.date:
    """
    Determina a data de referência para atualização dos dados.

    Antes das 18h BRT, retorna o dia útil anterior (permite rodar
    manualmente durante o dia para pegar dados de ontem).
    A partir das 18h, retorna hoje se dia útil.
    Em fins de semana/feriados, retorna o último dia útil.
    """
    now = yd.agora()
    today = now.date()

    if not yd.du.eh_dia_util(today):
        return yd.du.ultimo_dia_util()
    if now.hour < 18:
        return yd.du.deslocar(today, -1)
    return today


def is_special_holiday(date: dt.date) -> bool:
    """Não tem pregão no dia 24/12 e 31/12."""
    pre_xmas = dt.date(date.year, 12, 24)
    pre_ny = dt.date(date.year, 12, 31)
    return date in (pre_xmas, pre_ny)


def dates_to_update() -> list[dt.date]:
    """Retorna dias úteis a processar, do mais recente ao mais antigo."""
    last_date = determine_target_date()
    logger.info(f"Last trade date to update: {last_date}")
    start_date = yd.du.deslocar(last_date, -4)
    return [
        d
        for d in reversed(yd.du.gerar(start_date, last_date).to_list())
        if not is_special_holiday(d)
    ]


def dataset_has_date(config: DatasetConfig, target_date: dt.date) -> bool:
    """Checa se o parquet já contém dados para a target_date."""
    if not config.parquet_path.exists():
        return False
    df = pl.read_parquet(config.parquet_path, columns=config.id_cols[:1])
    date_col = config.id_cols[0]
    return df.filter(pl.col(date_col) == target_date).height > 0


def upsert_dataset(target_date: dt.date, config: DatasetConfig) -> bool | None:
    """
    Atualiza um dataset parquet com novos dados.

    Returns:
        True: dataset atualizado com sucesso.
        None: sem dados disponíveis para a data (fonte vazia).
        False: falha ao buscar ou processar os dados.
    """
    if not config.parquet_path.exists():
        logger.error(
            f"Missing base dataset for {config.dataset_name}: {config.parquet_path}. "
            "Refusing to recreate from scratch to avoid release history reset."
        )
        return False

    try:
        df_new = config.fetch_function(target_date)
    except Exception as e:
        logger.error(f"Failed to fetch {config.dataset_name} for {target_date}: {e}")
        return False

    if df_new.is_empty():
        return None

    df = pl.read_parquet(config.parquet_path)
    cols = [c for c in df.columns if c in df_new.columns]
    (
        pl.concat([df, df_new.select(cols)], how="vertical_relaxed")
        .unique(subset=config.id_cols, keep="last")
        .sort(config.id_cols)
        .write_parquet(config.parquet_path)
    )

    logger.info(f"{config.dataset_name} dataset updated with data from {target_date}")
    return True


def main() -> None:
    dates = dates_to_update()
    if not dates:
        logger.info("No trade updates on Christmas Eve or New Year's Eve.")
        return

    all_configs = [TPF_CONFIG, FUTURES_CONFIG]

    failed = []
    for date in dates:
        pending = [c for c in all_configs if not dataset_has_date(c, date)]
        if not pending:
            logger.info(f"All datasets already up to date for {date}.")
            continue
        for config in pending:
            if upsert_dataset(date, config) is False:
                failed.append((date, config))

    if failed:
        names = ", ".join(f"{c.dataset_name}@{d}" for d, c in failed)
        raise RuntimeError(f"Failed to update: {names}")


if __name__ == "__main__":
    main()
