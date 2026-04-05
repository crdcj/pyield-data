import datetime as dt
import logging
from calendar import monthrange
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import polars as pl
import pyield as yd
import requests

# Set precision (optional)
getcontext().prec = 28

# Configurations and constants
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
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
                    release_date = dt.datetime.strptime(
                        item["data_divulgacao"],
                        "%d/%m/%Y %H:%M:%S",
                    ).date()
                    calendario_ipca.append(release_date)
                except ValueError as e:
                    logger.warning(f"Invalid date format: {e}")

        calendario_ipca.sort()
        return pl.DataFrame({"data_divulgacao": calendario_ipca})

    except requests.RequestException as e:
        logger.error(f"Error fetching IPCA calendar: {e}")
        raise


def get_ipca_data(months_back: int = 4) -> Optional[float]:
    """
    Get IPCA data for the specified period.

    Args:
        months_back: Number of months to look back

    Returns:
        float: IPCA value as percentage or None if error occurs
    """
    try:
        today = dt.date.today()
        end_date = today.strftime("%d-%m-%Y")

        year, month = today.year, today.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        day = min(today.day, monthrange(year, month)[1])
        start_date = dt.date(year, month, day).strftime("%d-%m-%Y")

        df_ipca = yd.ipca.indices(start_date, end_date)

        if len(df_ipca) < 2:
            logger.warning("Not enough IPCA data points available")
            return None

        ipca_value = (df_ipca["Value"][-1] / df_ipca["Value"][-2]) - 1
        ipca_value = float(ipca_value) * 100
        return ipca_value

    except Exception as e:
        logger.error(f"Error fetching IPCA data: {e}")
        return None


def get_current_month_release_date(df_calendario: pl.DataFrame) -> Optional[dt.date]:
    """
    Find the current month's IPCA release date.

    Args:
        df_calendario: DataFrame with IPCA calendar data

    Returns:
        dt.date: Current month's release date or None if not found
    """
    today = dt.date.today()

    current_month_release = df_calendario.filter(
        pl.col("data_divulgacao").dt.month() == today.month,
        pl.col("data_divulgacao").dt.year() == today.year,
    )

    if current_month_release.is_empty():
        logger.warning(f"No IPCA release date found for {today.month}/{today.year}")
        return None

    return current_month_release["data_divulgacao"][0]


def get_previous_15th(date):
    """
    Given a date, returns the previous 15th day of a month.
    If the date is the 15th, returns the 15th of the previous month.
    """
    # date = pd.to_datetime(date)  # Ensure date is in datetime format

    # If the date is on or after the 15th of the current month
    if date.day >= 15:
        # Return the 15th of the current month
        return dt.datetime(date.year, date.month, 15)
    else:
        # We need the 15th of the previous month
        # If current month is January, go to December of previous year
        if date.month == 1:
            return dt.datetime(date.year - 1, 12, 15)
        else:
            return dt.datetime(date.year, date.month - 1, 15)


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
    today = dt.date.today()

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

    business_days = yd.du.gerar(last_date_in_df, today, fechamento="right").to_list()

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
        # Default to None if we couldn't get values
        inflation_value = None

        # Determine which inflation value to use based on the rules
        if (
            current_month_release_date is not None
            and ipca_value is not None
            and anbima_value is not None
        ):
            if date.day < 15:  # Before the 15th of the month
                if date >= current_month_release_date:
                    # After IPCA release, use IPCA value
                    inflation_value = ipca_value
                else:
                    # Before IPCA release, use ANBIMA value
                    inflation_value = anbima_value
            else:  # After the 15th of the month
                # Use ANBIMA value
                inflation_value = anbima_value
        elif anbima_value is not None:
            # Fallback to ANBIMA if we couldn't determine the rule
            inflation_value = anbima_value
        elif ipca_value is not None:
            # Fallback to IPCA if ANBIMA is not available
            inflation_value = ipca_value

        if inflation_value is None:
            logger.warning(f"No inflation value available for {date}, skipping")
            continue

        # Update vna. First get the last vna in the last 15th
        vna_base_date = get_previous_15th(date).date()
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


def is_business_day(date: dt.date) -> bool:
    """Check if the given date is a business day."""
    return yd.du.deslocar(date, 0) == date


def is_pre_holiday(date: dt.date) -> bool:
    """Check if the given date is the day before Christmas or New Year's Eve."""
    pre_xmas = dt.date(date.year, 12, 24)
    pre_ny = dt.date(date.year, 12, 31)
    return date == pre_xmas or date == pre_ny


def main():
    today = dt.date.today()

    # Check if today is a business day
    if not is_business_day(today):
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
        try:
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

        except FileNotFoundError:
            logger.info("No existing data found")

    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    main()
