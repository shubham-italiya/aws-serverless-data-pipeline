"""Lambda, triggered when a file lands in raw/: check it, then move it to validated/ or quarantine/.

Environment: DATA_BUCKET (the bucket) and ALERT_TOPIC_ARN (SNS topic for alerts).
"""
from __future__ import annotations

import io
import json
import os
from urllib.parse import unquote_plus

import boto3
import pandas as pd

from . import layout
from .checks import CheckResult, Report, batch_stats, run_checks

HISTORY_KEEP = 60
_clients: dict = {}


def _client(name: str):
    if name not in _clients:  # created once per Lambda container, reused across invocations
        _clients[name] = boto3.client(name)
    return _clients[name]


def handler(event, context=None) -> dict:
    """S3 'ObjectCreated' event -> one report per file."""
    reports = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        report = process_object(_client("s3"), _client("sns"), bucket, key, os.environ.get("ALERT_TOPIC_ARN"))
        reports.append({"key": key, "status": report.status})
    return {"processed": reports}


def process_object(s3, sns, bucket: str, key: str, topic_arn: str | None) -> Report:
    day = layout.batch_date(key)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        df = pd.read_csv(io.BytesIO(body), dtype={"Invoice": str, "StockCode": str})
        history = load_history(s3, bucket)
        report = run_checks(df, day, history)
    except (pd.errors.ParserError, UnicodeDecodeError, pd.errors.EmptyDataError) as e:
        df = None
        report = Report(day.isoformat(), 0, [CheckResult("readable", False, "error", f"cannot read CSV: {e}")])

    s3.put_object(Bucket=bucket, Key=layout.key_for(layout.REPORTS, day, "report.json"),
                  Body=json.dumps(report.to_dict(), indent=2).encode(), ContentType="application/json")
    target = layout.QUARANTINE if report.status == "rejected" else layout.VALIDATED
    s3.copy_object(Bucket=bucket, Key=layout.key_for(target, day), CopySource={"Bucket": bucket, "Key": key})
    s3.delete_object(Bucket=bucket, Key=key)

    if report.status != "rejected":
        save_history(s3, bucket, history, {"date": day.isoformat(), **batch_stats(df)})
    if report.failures and topic_arn:
        sns.publish(TopicArn=topic_arn, Subject=_subject(report), Message=_message(report, bucket))
    return report


def load_history(s3, bucket: str) -> list[dict]:
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=layout.PROFILE_KEY)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return []


def save_history(s3, bucket: str, history: list[dict], entry: dict) -> None:
    kept = [h for h in history if h["date"] != entry["date"]] + [entry]
    kept = sorted(kept, key=lambda h: h["date"])[-HISTORY_KEEP:]
    s3.put_object(Bucket=bucket, Key=layout.PROFILE_KEY, Body=json.dumps(kept).encode())


def _subject(report: Report) -> str:
    word = "REJECTED" if report.status == "rejected" else "Warning"
    return f"[transactions] {word}: batch {report.batch_date}"[:100]


def _message(report: Report, bucket: str) -> str:
    lines = [f"Batch {report.batch_date}: {report.status} ({report.rows} rows)", ""]
    lines += [f"- {r.name} ({r.severity}): {r.detail}" for r in report.failures]
    lines += ["", f"Full report: s3://{bucket}/{layout.REPORTS}date={report.batch_date}/report.json"]
    return "\n".join(lines)
