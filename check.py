import logging
import sys

from update import (
    FUTURES_CONFIG,
    TPF_CONFIG,
    dataset_has_date,
    determine_target_date,
    is_special_holiday,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def main() -> None:
    date = determine_target_date()
    logger.info(f"Checking data for {date}")

    if is_special_holiday(date):
        logger.info(f"{date} is a special holiday, no data expected.")
        return

    missing = [
        c.dataset_name
        for c in [TPF_CONFIG, FUTURES_CONFIG]
        if not dataset_has_date(c, date)
    ]

    if missing:
        logger.error(f"Missing data for {date}: {', '.join(missing)}")
        sys.exit(1)

    logger.info(f"All datasets present for {date}.")


if __name__ == "__main__":
    main()
