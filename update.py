"""Atualiza os datasets publicados no release do GitHub.

O workflow baixa os parquets do último release para `release_staging/`, este
script acrescenta os pregões que faltam e o workflow publica tudo de novo.
"""

import datetime as dt
import logging
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple

import polars as pl
import pyield as yd
from pyield.anbima import taxas as anbima_taxas

RELEASE_DIR = Path(__file__).parent / "release_staging"
FUTURES_TICKERS = ["DI1", "DDI", "FRC", "FRO", "DAP", "DOL", "WDO", "IND", "WIN"]
# A B3 republica o boletim até ~20h45 BRT (as versões anteriores trazem ajustes
# preliminares) e a ANBIMA publica até ~19h15. Antes do corte, o dia não é buscado.
CUTOFF_HOUR = 22

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Dataset(NamedTuple):
    name: str
    path: Path
    fetch: Callable[[dt.date], pl.DataFrame]
    id_cols: list[str]  # a primeira é a coluna de data


def fetch_futures(date: dt.date) -> pl.DataFrame:
    return yd.b3.boletim.buscar(
        data=date,
        prefixo_ticker=FUTURES_TICKERS,
        comprimento_ticker=6,
        boletim_completo=True,
    )


DATASETS = [
    Dataset(
        name="TPF",
        path=RELEASE_DIR / "anbima_tpf.parquet",
        fetch=anbima_taxas.buscar,
        id_cols=["data_referencia", "titulo", "data_vencimento"],
    ),
    Dataset(
        name="B3 Futures",
        path=RELEASE_DIR / "b3_futures.parquet",
        fetch=fetch_futures,
        id_cols=["TradDt", "TckrSymb"],
    ),
]


def target_date() -> dt.date:
    """Pregão mais recente cujos dados definitivos já deveriam estar publicados."""
    now = yd.agora()
    if now.hour < CUTOFF_HOUR:
        return yd.b3.pregao_anterior(now.date())
    return yd.b3.ultimo_pregao(now.date())


def dates_to_update() -> list[dt.date]:
    """Pregões dos últimos 5 dias úteis (retenção pública da ANBIMA).

    Reprocessar a janela recupera sozinho os dias em que a fonte falhou.
    """
    last_date = target_date()
    start_date = yd.du.deslocar(last_date, -4)
    return [
        d for d in yd.du.gerar(start_date, last_date).to_list() if yd.b3.eh_pregao(d)
    ]


def missing_dates(dataset: Dataset, dates: list[dt.date]) -> list[dt.date]:
    """Datas que ainda não estão no parquet do dataset."""
    date_col = dataset.id_cols[0]
    present = set(pl.read_parquet(dataset.path, columns=[date_col])[date_col])
    return [d for d in dates if d not in present]


def update_dataset(dataset: Dataset, dates: list[dt.date]) -> None:
    """Acrescenta ao parquet as datas ausentes que a fonte conseguir entregar.

    Fonte fora do ar ou ainda sem dados não derruba o job: o que foi obtido é
    publicado, a próxima execução agendada tenta de novo e o check.py acusa se
    a data continuar faltando.
    """
    df = pl.read_parquet(dataset.path)
    new_frames = []
    for date in missing_dates(dataset, dates):
        try:
            df_new = dataset.fetch(date)
        except Exception:
            logger.exception(f"Failed to fetch {dataset.name} for {date}")
            continue
        if df_new.is_empty():
            logger.warning(f"No {dataset.name} data available for {date}")
            continue
        logger.info(f"{dataset.name} {date}: {df_new.height} rows")
        new_frames.append(df_new.select(df.columns))

    if new_frames:
        (
            pl.concat([df, *new_frames], how="vertical_relaxed")
            .unique(subset=dataset.id_cols, keep="last")
            .sort(dataset.id_cols)
            .write_parquet(dataset.path)
        )


def main() -> None:
    dates = dates_to_update()
    logger.info(f"Trade dates to check: {dates[0]} to {dates[-1]}")

    for dataset in DATASETS:
        update_dataset(dataset, dates)


if __name__ == "__main__":
    main()
