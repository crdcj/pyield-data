"""
Job considera que a execução é feita diariamente e de madrugada no horário de Brasília.
Com isso, a data tem que ser ajustada para o dia anterior.
"""

import datetime as dt

import pandas as pd

# Configurações e constantes
BC_FILEPATH = "bc_secundario.parquet"


def update_dataset(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.concat([base_df, new_df])
    key_columns = ["DATA MOV", "SIGLA", "VENCIMENTO"]
    df.drop_duplicates(subset=key_columns, keep="last", inplace=True)
    return df.sort_values(by=key_columns).reset_index(drop=True)


def build_url(target_date: dt.date) -> str:
    """
    URL com todos os arquivos disponíveis:
    https://www4.bcb.gov.br/pom/demab/negociacoes/apresentacao.asp?frame=1

    Exemplo de URL para download:
    https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202408.ZIP

    File format: NegEYYYYMM.ZIP
    """
    file_date = target_date.strftime("%Y%m")
    file_name = f"NegE{file_date}.ZIP"
    base_url = "https://www4.bcb.gov.br/pom/demab/negociacoes/download/"
    return f"{base_url}{file_name}"


def fetch_data_from_url(file_url):
    df = pd.read_csv(file_url, sep=";", decimal=",", dtype_backend="numpy_nullable")
    date_cols = ["DATA MOV", "EMISSAO", "VENCIMENTO"]
    df[date_cols] = df[date_cols].apply(
        pd.to_datetime, format="%d/%m/%Y", errors="coerce"
    )
    return df


def run():
    today = dt.date.today()
    # target date is yesterday
    target_date = today - dt.timedelta(days=1)
    # Force a specific date for testing purposes
    # target_date = pd.to_datetime("12-08-2024", dayfirst=True)

    file_url = build_url(target_date)
    new_df = fetch_data_from_url(file_url)

    # Update dataset
    main_df = pd.read_parquet(BC_FILEPATH)
    updated_df = update_dataset(main_df, new_df)

    # Save updated dataset
    updated_df.to_parquet(BC_FILEPATH, index=False, compression="zstd")


if __name__ == "__main__":
    run()
