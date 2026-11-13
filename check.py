"""Falha se o último release não tiver todos os pregões da janela de atualização."""

import sys

from update import DATASETS, dates_to_update, missing_dates


def main() -> None:
    dates = dates_to_update()
    missing = [
        f"{dataset.name}@{date}"
        for dataset in DATASETS
        for date in missing_dates(dataset, dates)
    ]
    if missing:
        sys.exit(f"Missing data: {', '.join(missing)}")
    print(f"All datasets complete from {dates[0]} to {dates[-1]}.")


if __name__ == "__main__":
    main()
