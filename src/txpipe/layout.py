"""Where things live in the data bucket. One bucket, one folder ("prefix") per stage."""
from __future__ import annotations

import re
from datetime import date

RAW = "raw/"                 # files land here; the upload triggers the validate Lambda
VALIDATED = "validated/"     # batches that passed all error-level checks
QUARANTINE = "quarantine/"   # rejected batches, kept for inspection
REPORTS = "reports/"         # one JSON data-quality report per batch
CURATED = "curated/"         # Parquet data lake written by the Glue job
PROFILE_KEY = "state/profile.json"  # recent accepted batches, used for volume checks

_DATE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")


def batch_date(key: str) -> date:
    """raw/date=2011-09-01/transactions.csv -> 2011-09-01"""
    m = _DATE.search(key)
    if not m:
        raise ValueError(f"no date=YYYY-MM-DD folder in {key!r}")
    return date.fromisoformat(m[1])


def key_for(stage: str, day: date, name: str = "transactions.csv") -> str:
    return f"{stage}date={day.isoformat()}/{name}"
