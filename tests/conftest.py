from datetime import date
from types import SimpleNamespace

import boto3
import numpy as np
import pandas as pd
import pytest
from moto import mock_aws

REGION = "eu-west-2"


@pytest.fixture(autouse=True)
def fake_aws_credentials(monkeypatch):
    """moto intercepts every AWS call; these dummy values just stop boto3 looking for real credentials."""
    for name, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                        "AWS_SESSION_TOKEN": "testing", "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def aws():
    """A fake AWS account: data bucket, SNS topic, and an SQS queue subscribed to it (to read the alerts)."""
    with mock_aws():
        s3, sns, sqs = (boto3.client(n, region_name=REGION) for n in ("s3", "sns", "sqs"))
        s3.create_bucket(Bucket="data", CreateBucketConfiguration={"LocationConstraint": REGION})
        topic = sns.create_topic(Name="alerts")["TopicArn"]
        queue = sqs.create_queue(QueueName="alerts")["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn)
        yield SimpleNamespace(s3=s3, sns=sns, sqs=sqs, bucket="data", topic=topic, queue=queue)


def make_batch(day: date, rows: int = 60, seed: int = 0) -> pd.DataFrame:
    """A small, valid daily batch in the source file's format."""
    rng = np.random.default_rng(seed)
    times = pd.Timestamp(day) + pd.to_timedelta(rng.integers(8 * 3600, 18 * 3600, rows), unit="s")
    return pd.DataFrame({
        "Invoice": [str(500000 + i // 4) for i in range(rows)],
        "StockCode": [str(20000 + i) for i in range(rows)],
        "Description": [f"ITEM {i}" for i in range(rows)],
        "Quantity": rng.integers(1, 24, rows),
        "InvoiceDate": times.strftime("%Y-%m-%d %H:%M:%S"),
        "Price": np.round(rng.uniform(0.5, 9.0, rows), 2),
        "Customer ID": np.where(rng.random(rows) < 0.2, np.nan, rng.integers(12000, 18000, rows)),
        "Country": "United Kingdom",
    })


@pytest.fixture
def batch():
    return make_batch


def history(days: int = 20, rows: int = 60, sales: float = 3400.0) -> list[dict]:
    """Past accepted batches that vary like real days (rows +-10%, sales +-15%)."""
    rng = np.random.default_rng(1)
    return [{"date": f"2011-08-{d + 1:02d}", "rows": int(rows * rng.uniform(0.9, 1.1)),
             "sales": sales * rng.uniform(0.85, 1.15)} for d in range(days)]
