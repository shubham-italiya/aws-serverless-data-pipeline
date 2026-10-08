import json
from datetime import date

import pytest

from txpipe import validate_handler
from txpipe.validate_handler import handler

DAY = date(2011, 9, 1)
RAW_KEY = "raw/date=2011-09-01/transactions.csv"


@pytest.fixture
def run(aws, monkeypatch):
    monkeypatch.setattr(validate_handler, "_clients", {})   # create clients inside the fake AWS
    monkeypatch.setenv("ALERT_TOPIC_ARN", aws.topic)

    def upload_and_trigger(body: bytes) -> dict:
        aws.s3.put_object(Bucket=aws.bucket, Key=RAW_KEY, Body=body)
        event = {"Records": [{"s3": {"bucket": {"name": aws.bucket}, "object": {"key": RAW_KEY}}}]}
        return handler(event)
    return upload_and_trigger


def keys(aws) -> set[str]:
    return {o["Key"] for o in aws.s3.list_objects_v2(Bucket=aws.bucket).get("Contents", [])}


def alerts(aws) -> list[dict]:
    msgs = aws.sqs.receive_message(QueueUrl=aws.queue, MaxNumberOfMessages=10).get("Messages", [])
    return [json.loads(m["Body"]) for m in msgs]


def test_good_batch_is_validated_and_remembered(aws, run, batch):
    out = run(batch(DAY).to_csv(index=False).encode())
    assert out == {"processed": [{"key": RAW_KEY, "status": "passed"}]}
    assert keys(aws) == {"validated/date=2011-09-01/transactions.csv", "reports/date=2011-09-01/report.json",
                         "state/profile.json"}
    profile = json.loads(aws.s3.get_object(Bucket=aws.bucket, Key="state/profile.json")["Body"].read())
    assert profile[0]["date"] == "2011-09-01" and profile[0]["rows"] == 60
    assert alerts(aws) == []


def test_bad_batch_is_quarantined_and_alerted(aws, run, batch):
    out = run(batch(DAY).rename(columns={"Price": "UnitPrice"}).to_csv(index=False).encode())
    assert out["processed"][0]["status"] == "rejected"
    assert "quarantine/date=2011-09-01/transactions.csv" in keys(aws)
    assert not any(k.startswith(("raw/", "validated/", "state/")) for k in keys(aws))
    [alert] = alerts(aws)
    assert "REJECTED" in alert["Subject"] and "schema drift" in alert["Message"]


def test_unreadable_file_is_quarantined(aws, run):
    assert run(bytes(range(256)) * 10)["processed"][0]["status"] == "rejected"
    report = json.loads(aws.s3.get_object(Bucket=aws.bucket, Key="reports/date=2011-09-01/report.json")["Body"].read())
    assert report["results"][0]["name"] in ("readable", "schema")
