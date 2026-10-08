"""Glue Python shell job: turn validated CSV batches into a Parquet data lake.

  curated/transactions/date=YYYY-MM-DD/part-0.parquet   typed rows + amount + is_refund
  curated/daily_summary/date=YYYY-MM-DD/part-0.parquet  one row per day: invoices, customers, sales, refunds

Run in AWS Glue with --DATA_BUCKET <bucket>, or locally with:  python -m txpipe.curate_job --DATA_BUCKET <bucket>
Already-curated days are skipped, so the job can run on a schedule.
"""
from __future__ import annotations

import argparse
import io
import sys

import boto3
import pandas as pd

from . import layout

TX_PREFIX = f"{layout.CURATED}transactions/"
SUMMARY_PREFIX = f"{layout.CURATED}daily_summary/"


def clean(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "invoice": df["Invoice"].astype(str),
        "stock_code": df["StockCode"].astype(str),
        "description": df["Description"].astype("string"),
        "quantity": pd.to_numeric(df["Quantity"]).astype("int64"),
        "invoice_ts": pd.to_datetime(df["InvoiceDate"]),
        "price": pd.to_numeric(df["Price"]).astype("float64"),
        "customer_id": pd.to_numeric(df["Customer ID"]).astype("Int64"),
        "country": df["Country"].astype("string"),
    })
    out["amount"] = out["quantity"] * out["price"]
    out["is_refund"] = out["invoice"].str.startswith("C")
    return out


def summarise(tx: pd.DataFrame, day: str) -> pd.DataFrame:
    sales = tx.loc[~tx["is_refund"], "amount"].sum()
    refunds = -tx.loc[tx["is_refund"], "amount"].sum()
    return pd.DataFrame([{
        "date": pd.Timestamp(day), "rows": len(tx), "invoices": tx["invoice"].nunique(),
        "customers": tx["customer_id"].nunique(), "gross_sales": round(float(sales), 2),
        "refunds": round(float(refunds), 2), "net_sales": round(float(sales - refunds), 2),
    }])


def _list(s3, bucket: str, prefix: str) -> list[str]:
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, **({"ContinuationToken": token} if token else {})}
        page = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", [])]
        if not page.get("IsTruncated"):
            return keys
        token = page["NextContinuationToken"]


def _put_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def curate(s3, bucket: str) -> list[str]:
    """Curate every validated day that is not curated yet. Returns the days processed."""
    done = {layout.batch_date(k).isoformat() for k in _list(s3, bucket, SUMMARY_PREFIX)}
    processed = []
    for key in sorted(_list(s3, bucket, layout.VALIDATED)):
        day = layout.batch_date(key).isoformat()
        if day in done:
            continue
        raw = pd.read_csv(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()),
                          dtype={"Invoice": str, "StockCode": str})
        tx = clean(raw)
        _put_parquet(s3, bucket, f"{TX_PREFIX}date={day}/part-0.parquet", tx)
        _put_parquet(s3, bucket, f"{SUMMARY_PREFIX}date={day}/part-0.parquet", summarise(tx, day))
        processed.append(day)
    return processed


def _args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--DATA_BUCKET", required=True)
    return parser.parse_known_args(argv)[0]  # Glue adds its own arguments; ignore them


def main(argv: list[str] | None = None) -> None:
    args = _args(sys.argv[1:] if argv is None else argv)
    days = curate(boto3.client("s3"), args.DATA_BUCKET)
    print(f"Curated {len(days)} day(s): {', '.join(days) or 'nothing new'}")


if __name__ == "__main__":
    main()
