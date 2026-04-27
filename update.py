import datetime as dt
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyield as yd
from pyield.b3.boletim import boletim_negociacao

# Artefatos locais do workflow: o job baixa do latest release para
# `release_staging/`, atualiza em memoria e depois publica novamente no release.
BASE_DIR = Path(__file__).parent
RELEASE_DATA_DIR = BASE_DIR / "release_staging"
TPF_PARQUET = RELEASE_DATA_DIR / "anbima_tpf.parquet"
FUTURES_PARQUET = RELEASE_DATA_DIR / "b3_futures.parquet"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_futures_on_date(date: dt.date) -> pl.DataFrame:
    df = boletim_negociacao(
        data=date,
        prefixo_ticker=["DI1", "DDI", "FRC", "FRO", "DAP", "DOL", "WDO", "IND", "WIN"],
        comprimento_ticker=6,
        boletim_completo=True,
    )
    logger.info(f"B3 boletim_negociacao({date}): shape={df.shape}, cols={df.columns}")
    if df.is_empty():
        # Tentar sem filtro pra ver se o problema é no filtro ou na fonte
        df_raw = boletim_negociacao(data=date, boletim_completo=True)
        logger.info(f"B3 sem filtro: shape={df_raw.shape}")
        raise ValueError(f"No futures data available for {date}")

    return df


def get_tpf_on_date(date: dt.date) -> pl.DataFrame:
    return yd.tpf.taxas(data=date, completo=True)


@dataclass
class DatasetConfig:
    """Configuração para atualização de dataset."""

    parquet_path: Path
    fetch_function: Callable[[dt.date], pl.DataFrame]
    id_cols: list[str]
    dataset_name: str


def upsert_dataset(target_date: dt.date, config: DatasetConfig) -> None:
    """
    Atualiza um dataset parquet com novos dados.

    O processamento ocorre em memoria (`df` e `df_new`). O unico estado persistido
    e o arquivo parquet em `release_staging/`, que o workflow publica como
    asset do release.

    Args:
        target_date: Data dos dados a serem buscados
        config: Configuração do dataset a ser atualizado

    Raises:
        ValueError: Se não houver dados disponíveis para a data especificada
    """
    if not config.parquet_path.exists():
        raise FileNotFoundError(
            f"Missing base dataset for {config.dataset_name}: {config.parquet_path}. "
            "Refusing to recreate from scratch to avoid release history reset."
        )

    df = pl.read_parquet(config.parquet_path)

    df_new = config.fetch_function(target_date)

    if df_new.is_empty():
        raise ValueError(f"No {config.dataset_name} data available for {target_date}")

    cols = [c for c in df_new.columns if c in df.columns]
    (
        pl.concat([df, df_new.select(cols)], how="vertical_relaxed")
        .unique(subset=config.id_cols, keep="last")
        .sort(config.id_cols)
        .write_parquet(config.parquet_path)
    )

    logger.info(f"{config.dataset_name} dataset updated with data from {target_date}")


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

    if yd.du.eh_dia_util(today):
        if now.hour < 18:
            return yd.du.deslocar(today, -1)
        return today

    return yd.du.ultimo_dia_util()


def is_special_holiday(date: dt.date) -> bool:
    """Não tem pregão no dia 24/12 e 31/12."""
    pre_xmas = dt.date(date.year, 12, 24)
    pre_ny = dt.date(date.year, 12, 31)
    return date in (pre_xmas, pre_ny)


def dataset_has_date(config: DatasetConfig, target_date: dt.date) -> bool:
    """Checa se o parquet já contém dados para a target_date."""
    if not config.parquet_path.exists():
        return False
    df = pl.read_parquet(config.parquet_path, columns=config.id_cols[:1])
    date_col = config.id_cols[0]
    return df.filter(pl.col(date_col) == target_date).height > 0


def main() -> None:
    if len(sys.argv) > 1:
        target_date = dt.date.fromisoformat(sys.argv[1])
    else:
        target_date = determine_target_date()
    logger.info(f"Determined target trade date: {target_date}")

    if is_special_holiday(target_date):
        logger.info("No trade updates on Christmas Eve or New Year's Eve.")
        return

    all_configs = [TPF_CONFIG, FUTURES_CONFIG]
    pending = [c for c in all_configs if not dataset_has_date(c, target_date)]

    if not pending:
        logger.info("All datasets already up to date.")
        return

    failed = []
    for config in pending:
        try:
            upsert_dataset(target_date, config)
        except Exception as e:
            logger.error(f"Failed to update {config.dataset_name}: {e}")
            failed.append(config)

    if failed:
        names = ", ".join(c.dataset_name for c in failed)
        raise RuntimeError(f"Failed to update: {names}")


if __name__ == "__main__":
    main()
