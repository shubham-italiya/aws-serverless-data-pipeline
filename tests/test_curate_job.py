import io
from datetime import date

import pandas as pd

from txpipe.curate_job import curate, main


def put_validated(aws, df, day="2011-09-01"):
    aws.s3.put_object(Bucket=aws.bucket, Key=f"validated/date={day}/transactions.csv",
                      Body=df.to_csv(index=False).encode())


def read_parquet(aws, key):
    return pd.read_parquet(io.BytesIO(aws.s3.get_object(Bucket=aws.bucket, Key=key)["Body"].read()))


def test_curate_writes_typed_rows_and_a_daily_summary(aws, batch):
    df = batch(date(2011, 9, 1))
    df.loc[0, "Invoice"] = "C500000"           # a refund line
    df.loc[0, "Quantity"] = -df.loc[0, "Quantity"]
    put_validated(aws, df)
    assert curate(aws.s3, aws.bucket) == ["2011-09-01"]

    tx = read_parquet(aws, "curated/transactions/date=2011-09-01/part-0.parquet")
    assert len(tx) == 60 and tx["is_refund"].sum() == 1
    assert str(tx["invoice_ts"].dtype).startswith("datetime64") and str(tx["customer_id"].dtype) == "Int64"

    summary = read_parquet(aws, "curated/daily_summary/date=2011-09-01/part-0.parquet").iloc[0]
    amount = df["Quantity"] * df["Price"]
    assert summary["net_sales"] == round(amount.sum(), 2)
    assert summary["refunds"] == round(-amount.iloc[0], 2)


def test_curate_skips_days_already_done(aws, batch):
    put_validated(aws, batch(date(2011, 9, 1)))
    assert curate(aws.s3, aws.bucket) == ["2011-09-01"]
    put_validated(aws, batch(date(2011, 9, 2)), day="2011-09-02")
    assert curate(aws.s3, aws.bucket) == ["2011-09-02"]


def test_main_ignores_extra_glue_arguments(aws, batch, capsys):
    put_validated(aws, batch(date(2011, 9, 1)))
    main(["--DATA_BUCKET", aws.bucket, "--JOB_ID", "j_123", "--job-bookmark-option", "job-bookmark-disable"])
    assert "Curated 1 day(s): 2011-09-01" in capsys.readouterr().out
