import datetime as dt
import logging
from decimal import Decimal, getcontext
from pathlib import Path

import polars as pl
import pyield as yd
import requests
from dateutil.relativedelta import relativedelta

# Set precision (optional)
getcontext().prec = 28

# Configurations and constants

IBGE_CALENDAR_URL = "https://servicodados.ibge.gov.br/api/v3/calendario/"

# Local workflow staging folder (downloaded from/reuploaded to release assets)

try:
    # Try to use __file__ (works in scripts)
    base_dir = Path(__file__).parent
except NameError:
    # Fall back to current working directory (for interactive sessions)
    base_dir = Path.cwd()
release_staging_dir = base_dir / "release_staging"
VNA_BASE_CSV = release_staging_dir / "vna_base.csv"
VNA_PARQUET = release_staging_dir / "vna_ntnb.parquet"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_ipca_calendar() -> pl.DataFrame:
    """
    Fetch IPCA calendar data from IBGE API.

    Returns:
        pl.DataFrame: DataFrame containing IPCA release dates

    Raises:
        requests.RequestException: If API request fails
    """

    try:
        response = requests.get(IBGE_CALENDAR_URL)
        response.raise_for_status()

        calendario_completo = response.json()

        calendario_ipca = []

        for item in calendario_completo["items"]:
            if item["titulo"] == "Índice Nacional de Preços ao Consumidor Amplo":
                try:
                    release_date = dt.date.strptime(
                        item["data_divulgacao"],
                        "%d/%m/%Y %H:%M:%S",
                    )
                    calendario_ipca.append(release_date)
                except ValueError as e:
                    logger.warning(f"Invalid date format: {e}")

        calendario_ipca.sort()
        return pl.DataFrame({"data_divulgacao": calendario_ipca})

    except requests.RequestException as e:
        logger.error(f"Error fetching IPCA calendar: {e}")
        raise


def get_ipca_data(months_back: int = 4) -> float | None:
    """
    Get IPCA data for the specified period.

    Args:
        months_back: Number of months to look back

    Returns:
        float: IPCA value as percentage or None if error occurs
    """
    try:
        today = yd.hoje()
        end_date = today.strftime("%d-%m-%Y")
        start_date = (today - relativedelta(months=months_back)).strftime("%d-%m-%Y")

        df_ipca = yd.ipca.indice_serie(start_date, end_date)

        if len(df_ipca) < 2:
            logger.warning("Not enough IPCA data points available")
            return None

        ipca_value = (df_ipca["indice"][-1] / df_ipca["indice"][-2]) - 1
        ipca_value = float(ipca_value) * 100
        return ipca_value

    except Exception as e:  # noqa: BLE001 — falhas da fonte não interrompem o fallback
        logger.error(f"Error fetching IPCA data: {e}")
        return None


def get_current_month_release_date(df_calendario: pl.DataFrame) -> dt.date | None:
    """
    Find the current month's IPCA release date.

    Args:
        df_calendario: DataFrame with IPCA calendar data

    Returns:
        dt.date: Current month's release date or None if not found
    """
    today = yd.hoje()

    current_month_release = df_calendario.filter(
        pl.col("data_divulgacao").dt.month() == today.month,
        pl.col("data_divulgacao").dt.year() == today.year,
    )

    if current_month_release.is_empty():
        logger.warning(f"No IPCA release date found for {today.month}/{today.year}")
        return None

    return current_month_release["data_divulgacao"][0]


def get_latest_15th(date: dt.date) -> dt.date:
    """Return the most recent 15th, including the given date."""
    if date.day >= 15:
        return dt.date(date.year, date.month, 15)
    if date.month == 1:
        return dt.date(date.year - 1, 12, 15)
    return dt.date(date.year, date.month - 1, 15)


def update_vna_dataframe(
    df_vna_base: pl.DataFrame,
    df_vna: pl.DataFrame,
    df_calendario: pl.DataFrame,
) -> pl.DataFrame:
    """
    Update vna dataframe.

    Args:
        df_vna_base: Base VNA dataframe
        df_vna: Existing vna dataframe
        df_calendario: IPCA calendar dataframe

    Returns:
        pl.DataFrame: Updated vna dataframe
    """
    today = yd.hoje()

    # Get the last date in the dataframe
    last_date_raw = df_vna["reference_date"].max()
    if not isinstance(last_date_raw, dt.date):
        logger.warning("Empty VNA dataframe")
        return df_vna
    last_date_in_df = last_date_raw

    # Ensure last_date_in_df is before today
    if last_date_in_df >= today:
        logger.info(f"Data already up to date until {last_date_in_df}")
        return df_vna

    business_days = yd.du.gerar(
        last_date_in_df, today, limites_inclusivos="fim"
    ).to_list()

    if len(business_days) == 0:
        logger.info("No new business days to add")
        return df_vna

    # Get the current month's IPCA release date
    current_month_release_date = get_current_month_release_date(df_calendario)

    # Get the ANBIMA projection
    anbima_value = yd.ipca.taxa_projetada().valor_projetado
    anbima_value = float(Decimal(f"{anbima_value}") * Decimal("100.00"))
    logger.info(f"ANBIMA projection: {anbima_value:.2f}%")

    # Get IPCA data
    ipca_value = get_ipca_data()
    if ipca_value is not None:
        logger.info(f"IPCA value: {ipca_value:.2f}%")

    # Create new rows for the dataframe
    new_rows = []

    for date in business_days:
        inflation_value = anbima_value
        if (
            current_month_release_date is not None
            and ipca_value is not None
            and current_month_release_date <= date
            and date.day < 15
        ):
            inflation_value = ipca_value

        # Update vna. First get the last vna in the last 15th
        vna_base_date = get_latest_15th(date)
        vna_base = df_vna_base.filter(pl.col("reference_date") == vna_base_date)["vna"][
            0
        ]

        if vna_base_date.month == 12:
            next_vna_base_date = dt.date(vna_base_date.year + 1, 1, 15)
        else:
            next_vna_base_date = dt.date(
                vna_base_date.year, vna_base_date.month + 1, 15
            )

        du_rf = yd.du.contar(vna_base_date, date)
        du_m = yd.du.contar(vna_base_date, next_vna_base_date)

        vna_du = vna_base * (1 + inflation_value / 100) ** (du_rf / du_m)
        vna_du = int(vna_du * 1000000) / 1000000

        dc_rf = (date - vna_base_date).days
        dc_m = (next_vna_base_date - vna_base_date).days

        vna_dc = vna_base * (1 + inflation_value / 100) ** (dc_rf / dc_m)
        vna_dc = int(vna_dc * 1000000) / 1000000

        new_rows.append(
            {
                "reference_date": date,
                "inflation": inflation_value,
                "vna_du": vna_du,
                "vna_dc": vna_dc,
            }
        )

    if not new_rows:
        logger.info("No new data to add")
        return df_vna

    new_data = pl.DataFrame(new_rows)

    updated_df = (
        pl.concat([df_vna, new_data], how="diagonal_relaxed")
        .unique(subset=["reference_date"], keep="last")
        .sort("reference_date")
    )

    logger.info(f"Added {len(new_data)} new data points")
    return updated_df


def is_pre_holiday(date: dt.date) -> bool:
    """Check for Christmas Eve or New Year's Eve."""
    return date.month == 12 and date.day in (24, 31)


def main():
    today = yd.hoje()

    # Check if today is a business day
    if not yd.du.eh_dia_util(today):
        logger.warning("Today is not a business day.")
        return

    # Check if today is a pre-holiday
    if is_pre_holiday(today):
        logger.warning(
            "There is no session on the day before Christmas or New Year's Eve. Aborting..."
        )
        return

    try:
        # Get IPCA calendar
        df_calendar = get_ipca_calendar()

        # Load existing vna data
        df_vna_base = pl.read_csv(VNA_BASE_CSV, try_parse_dates=True)
        df_vna = pl.read_parquet(VNA_PARQUET).with_columns(
            pl.col("reference_date").cast(pl.Date)
        )
        logger.info(f"Loaded existing data with {len(df_vna)} entries")

        # Update inflation dataframe
        df_vna_updated = update_vna_dataframe(df_vna_base, df_vna, df_calendar)

        # Save the updated dataframe to parquet
        df_vna_updated.write_parquet(VNA_PARQUET)
        logger.info(f"Updated data saved with {len(df_vna_updated)} entries")

    except Exception:
        logger.exception("Error in main process")
        raise


if __name__ == "__main__":
    main()
