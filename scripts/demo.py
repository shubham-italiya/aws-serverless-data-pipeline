"""End-to-end run on a fake AWS account (moto): every file goes through the real Lambda handler and Glue job code.

  python scripts/make_batches.py   # once
  python scripts/demo.py                                            # writes reports/demo.md and reports/demo.png
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from moto import mock_aws  # noqa: E402

from txpipe import validate_handler  # noqa: E402
from txpipe.curate_job import curate  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SAMPLES, REPORTS = ROOT / "sample_data", ROOT / "reports"
REGION, BUCKET = "eu-west-2", "demo-transactions"


def main() -> None:
    os.environ.update({"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                       "AWS_DEFAULT_REGION": REGION})
    files = [(f, "real day") for f in sorted((SAMPLES / "good").glob("date=*/transactions.csv"))]
    files += [(f, f.parent.parent.name.replace("_", " ")) for f in (SAMPLES / "broken").glob("*/date=*/*.csv")]
    files.sort(key=lambda x: x[0].parent.name)
    if not files:
        raise SystemExit("No sample data. Run scripts/make_batches.py first.")

    with mock_aws():
        s3, sns, sqs = (boto3.client(n, region_name=REGION) for n in ("s3", "sns", "sqs"))
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        topic = sns.create_topic(Name="alerts")["TopicArn"]
        queue = sqs.create_queue(QueueName="alerts")["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=queue, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=arn)
        os.environ["ALERT_TOPIC_ARN"] = topic
        validate_handler._clients.clear()

        rows = []
        for path, label in files:
            key = f"raw/{path.parent.name}/transactions.csv"
            s3.put_object(Bucket=BUCKET, Key=key, Body=path.read_bytes())
            out = validate_handler.handler({"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]})
            report = json.loads(s3.get_object(Bucket=BUCKET, Key=f"reports/{path.parent.name}/report.json")["Body"]
                                .read())
            failed = [f"{r['name']}: {r['detail']}" for r in report["results"] if not r["passed"]]
            rows.append({"date": path.parent.name[5:], "input": label, "rows": report["rows"],
                         "status": out["processed"][0]["status"], "why": "; ".join(failed)})

        curated_days = curate(s3, BUCKET)
        summary = pd.concat(pd.read_parquet(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read()))
                            for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="curated/daily_summary/")["Contents"])
        alerts = []
        while msgs := sqs.receive_message(QueueUrl=queue, MaxNumberOfMessages=10).get("Messages"):
            alerts += [json.loads(m["Body"]) for m in msgs]
            done = [{"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]} for m in msgs]
            sqs.delete_message_batch(QueueUrl=queue, Entries=done)

    write_report(pd.DataFrame(rows), summary.sort_values("date"), alerts, len(curated_days))


def write_report(results: pd.DataFrame, summary: pd.DataFrame, alerts: list[dict], curated: int) -> None:
    REPORTS.mkdir(exist_ok=True)
    counts = results.groupby(["input", "status"]).size().unstack(fill_value=0)
    flagged = results[results["status"] != "passed"]
    lines = [f"# Demo run: {len(results)} files through the pipeline", "",
             "Every file was uploaded to `raw/` in a fake AWS account (moto) and processed by the real Lambda handler;",
             "the Glue job code then curated the accepted days into Parquet.", "",
             "## Outcome by input", "", "| Input | " + " | ".join(counts.columns) + " |",
             "|---|" + "---|" * len(counts.columns)]
    lines += [f"| {i} | " + " | ".join(str(v) for v in r) + " |" for i, r in counts.iterrows()]
    lines += ["", f"SNS alerts sent: **{len(alerts)}**. Days curated to Parquet: **{curated}**.", "",
              "## Every batch that was not a clean pass", "", "| Date | Input | Rows | Status | Why |",
              "|---|---|---|---|---|"]
    lines += [f"| {r.date} | {r.input} | {r.rows:,} | {r.status} | {r.why} |" for r in flagged.itertuples()]
    example = next((a for a in alerts if "REJECTED" in a["Subject"]), alerts[0] if alerts else None)
    if example:
        lines += ["", "## Example alert (SNS message)", "", "```", example["Subject"], "", example["Message"], "```"]
    (REPORTS / "demo.md").write_text("\n".join(lines) + "\n")

    fig, ax = plt.subplots(figsize=(10, 3.4))
    ax.bar(summary["date"], summary["net_sales"] / 1000, color="#2563eb", width=0.8, label="Accepted day (curated)")
    for d in pd.to_datetime(results.loc[results["status"] == "rejected", "date"]):
        ax.axvline(d, color="#dc2626", lw=1.2, ls="--")
    ax.plot([], [], color="#dc2626", ls="--", label="Rejected batch (quarantined)")
    ax.set_ylabel("Net sales, £ thousands")
    ax.set_title("Curated daily summary from the Parquet data lake, with rejected batches marked")
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    fig.savefig(REPORTS / "demo.png", dpi=150)
    plt.close(fig)
    print("\n".join(lines[:16]))


if __name__ == "__main__":
    main()
