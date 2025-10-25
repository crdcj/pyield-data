import datetime as dt
import logging
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pyield as yd
import requests

# Set precision (optional)
getcontext().prec = 28

# Configurations and constants
BZ_TIMEZONE = ZoneInfo("America/Sao_Paulo")
IBGE_CALENDAR_URL = "https://servicodados.ibge.gov.br/api/v3/calendario/"

# Files are in the data folder

try:
    # Try to use __file__ (works in scripts)
    base_dir = Path(__file__).parent
except NameError:
    # Fall back to current working directory (for interactive sessions)
    base_dir = Path.cwd()
data_dir = base_dir / "data"
VNA_BASE_CSV = data_dir / "vna_base.csv"
VNA_PARQUET = data_dir / "vna_ntnb.parquet"

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_ipca_calendar() -> pd.DataFrame:
    """
    Fetch IPCA calendar data from IBGE API.

    Returns:
        pd.DataFrame: DataFrame containing IPCA release dates

    Raises:
        requests.RequestException: If API request fails
    """

    try:
        # Make request to the API
        response = requests.get(IBGE_CALENDAR_URL)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Convert response to JSON
        calendario_completo = response.json()

        calendario_ipca = []

        for i in range(len(calendario_completo["items"])):
            # Verificando se o título ou a descrição contém "IPCA"
            if (
                calendario_completo["items"][i]["titulo"]
                == "Índice Nacional de Preços ao Consumidor Amplo"
            ):
                try:
                    release_date = pd.to_datetime(
                        calendario_completo["items"][i]["data_divulgacao"],
                        format="%d/%m/%Y %H:%M:%S",
                    ).date()
                    calendario_ipca.append(release_date)
                except (ValueError, pd.errors.OutOfBoundsDatetime) as e:
                    logger.warning(f"Invalid date format: {e}")

        calendario_ipca.sort()
        df_calendario = pd.DataFrame({"data_divulgacao": calendario_ipca})
        return df_calendario

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
        today = pd.Timestamp.today().date()
        end_date = today.strftime("%d-%m-%Y")
        start_date = (today - pd.DateOffset(months=months_back)).strftime("%d-%m-%Y")

        df_ipca = yd.ipca.indexes(start_date, end_date)

        if len(df_ipca) < 2:
            logger.warning("Not enough IPCA data points available")
            return None

        ipca_value = (df_ipca["Value"].iloc[-1] / df_ipca["Value"].iloc[-2]) - 1
        ipca_value = float(ipca_value) * 100
        return ipca_value

    except Exception as e:
        logger.error(f"Error fetching IPCA data: {e}")
        return None


def get_current_month_release_date(df_calendario: pd.DataFrame) -> Optional[dt.date]:
    """
    Find the current month's IPCA release date.

    Args:
        df_calendario: DataFrame with IPCA calendar data

    Returns:
        dt.date: Current month's release date or None if not found
    """
    today = pd.Timestamp.today().date()
    current_month = today.month
    current_year = today.year

    # Find the most recent IPCA release date
    ipca_release_dates = pd.to_datetime(df_calendario["data_divulgacao"])
    current_month_release = ipca_release_dates[
        (ipca_release_dates.dt.month == current_month)
        & (ipca_release_dates.dt.year == current_year)
    ]

    if len(current_month_release) == 0:
        logger.warning(f"No IPCA release date found for {current_month}/{current_year}")
        return None

    return current_month_release.iloc[0].date()


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
    df_vna_base: pd.DataFrame,
    df_vna: pd.DataFrame,
    df_calendario: pd.DataFrame,
) -> pd.DataFrame:
    """
    Update vna dataframe.

    Args:
        df_vna: Existing vna dataframe
        df_calendario: IPCA calendar dataframe

    Returns:
        pd.DataFrame: Updated vna dataframe
    """
    today = pd.Timestamp.today().date()

    # Get the last date in the dataframe
    last_date_in_df = pd.Timestamp(df_vna["reference_date"].max()).date()

    # Ensure last_date_in_df is before today
    if last_date_in_df >= today:
        logger.info(f"Data already up to date until {last_date_in_df}")
        return df_vna

    business_days = yd.bday.generate(
        last_date_in_df, today, inclusive="right"
    ).to_list()

    if len(business_days) == 0:
        logger.info("No new business days to add")
        return df_vna

    # Get the current month's IPCA release date
    current_month_release_date = get_current_month_release_date(df_calendario)

    # Get the ANBIMA projection
    anbima_value = yd.ipca.projected_rate().projected_value
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

        # Update vna. First get the last vna in the last 15th
        vna_base_date = get_previous_15th(date).date()
        vna_base = df_vna_base.query(f'reference_date == "{vna_base_date}"')[
            "vna"
        ].values[0]

        next_vna_base_date = vna_base_date + pd.DateOffset(months=1)
        next_vna_base_date = next_vna_base_date.replace(day=15).date()

        du_rf = yd.bday.count(vna_base_date, date)
        du_m = yd.bday.count(vna_base_date, next_vna_base_date)

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

    # Create a dataframe from the new rows
    new_data = pd.DataFrame(new_rows)

    if new_data.empty:
        logger.info("No new data to add")
        return df_vna

    # Concatenate with the original dataframe
    updated_df = pd.concat([df_vna, new_data], ignore_index=True)

    # Remove any duplicates based on reference_date
    updated_df = updated_df.drop_duplicates(subset=["reference_date"], keep="last")

    # Ensure reference_date is datetime
    updated_df["reference_date"] = pd.to_datetime(updated_df["reference_date"])

    # Sort by reference_date
    updated_df = updated_df.sort_values("reference_date").reset_index(drop=True)

    logger.info(f"Added {len(new_data)} new data points")
    return updated_df


def is_business_day(date: dt.date) -> bool:
    """Check if the given date is a business day."""
    return yd.bday.offset(date, 0) == date


def is_pre_holiday(date: dt.date) -> bool:
    """Check if the given date is the day before Christmas or New Year's Eve."""
    pre_xmas = dt.date(date.year, 12, 24)
    pre_ny = dt.date(date.year, 12, 31)
    return date == pre_xmas or date == pre_ny


def main():
    today = pd.Timestamp.today().date()

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
            df_vna_base = pd.read_csv(VNA_BASE_CSV)
            df_vna = pd.read_parquet(VNA_PARQUET)
            df_vna_base["reference_date"] = pd.to_datetime(
                df_vna_base["reference_date"]
            )
            logger.info(f"Loaded existing data with {len(df_vna)} entries")

            # Update inflation dataframe
            df_vna_updated = update_vna_dataframe(df_vna_base, df_vna, df_calendar)

            # Save the updated dataframe to parquet
            df_vna_updated.to_parquet(
                VNA_PARQUET,
                compression="gzip",
                index=False,
            )
            logger.info(f"Updated data saved with {len(df_vna_updated)} entries")

        except (FileNotFoundError, pd.errors.EmptyDataError):
            logger.info("No existing data found")

    except Exception as e:
        logger.error(f"Error in main process: {e}", exc_info=True)


if __name__ == "__main__":
    main()
