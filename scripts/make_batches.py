"""Make sample input: one CSV per trading day from the real Online Retail II data, plus broken copies.

  python scripts/make_batches.py            # downloads the dataset (~45 MB) into data/ if needed

Writes sample_data/good/date=.../transactions.csv (Sep-Oct 2011) and sample_data/broken/<case>/date=.../
transactions.csv: real November days damaged on purpose, one kind of fault each, to show what the checks catch.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "sample_data"
URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"  # Chen (2019), CC BY 4.0


def download(dest: Path) -> None:
    import tempfile
    import urllib.request
    import zipfile

    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "retail.zip"
        urllib.request.urlretrieve(URL, archive)
        with zipfile.ZipFile(archive) as z:
            dest.write_bytes(z.read(next(n for n in z.namelist() if n.endswith(".xlsx"))))


def write(df: pd.DataFrame, folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    df.to_csv(folder / "transactions.csv", index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "online_retail_II.xlsx",
                    help="online_retail_II.xlsx; downloaded from the UCI repository if missing")
    args = ap.parse_args()
    if not args.source.exists():
        download(args.source)

    tx = pd.concat(pd.read_excel(args.source, sheet_name=None).values(), ignore_index=True)
    tx = tx[(tx["InvoiceDate"] >= "2011-09-01") & (tx["InvoiceDate"] < "2011-11-10")]
    tx["InvoiceDate"] = tx["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
    days = {d: g for d, g in tx.groupby(tx["InvoiceDate"].str[:10])}

    good = [d for d in days if d < "2011-11-01"]
    for d in good:
        write(days[d], OUT / "good" / f"date={d}")

    rng = np.random.default_rng(0)
    nov = sorted(d for d in days if d >= "2011-11-01")

    def broken(case: str, day: str, df: pd.DataFrame) -> None:
        write(df, OUT / "broken" / case / f"date={day}")

    broken("schema_drift", nov[0], days[nov[0]].rename(columns={"Price": "UnitPrice"}))
    df = days[nov[1]].copy()
    df.loc[df.sample(frac=0.05, random_state=0).index, "Quantity"] = np.nan
    broken("missing_quantities", nov[1], df)
    df = days[nov[2]].copy()
    df.loc[df.sample(n=3, random_state=1).index, "Price"] *= -1
    broken("negative_prices", nov[2], df)
    broken("partial_upload", nov[3], days[nov[3]].head(int(len(days[nov[3]]) * 0.04)))
    broken("file_sent_twice", nov[4], pd.concat([days[nov[4]], days[nov[4]]]))
    broken("previous_day_mixed_in", nov[5], pd.concat([days[nov[5]], days[nov[4]].head(50)]))
    folder = OUT / "broken" / "unreadable" / f"date={nov[6]}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "transactions.csv").write_bytes(rng.bytes(2000))
    print(f"{len(good)} good days, 7 broken files -> {OUT}")


if __name__ == "__main__":
    main()
