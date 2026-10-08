"""Data-quality checks for one daily batch of transactions.

Each check returns a CheckResult. A failed "error" check rejects the batch (it goes to quarantine and an
alert is sent); a failed "warning" check lets the batch through but still sends an alert.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date

import numpy as np
import pandas as pd

EXPECTED_COLUMNS = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID",
                    "Country"]
# Limits were set from 518 days of history (Dec 2009 - Aug 2011, before the demo period): errors at about the
# 99th percentile of normal days, warnings at about the 95th. Key fields must never be empty. Guest orders have
# no customer ID: on a median day 19% are empty, on the 99th-percentile day 67%.
MAX_NULL_RATE = {"Invoice": 0.0, "StockCode": 0.0, "Quantity": 0.0, "InvoiceDate": 0.0, "Price": 0.0,
                 "Country": 0.0, "Description": 0.06, "Customer ID": 0.70}
DUPLICATE_WARN = 0.035    # 95th percentile of normal days is 3.3%
DUPLICATE_ERROR = 0.20    # far above normal: the file was probably loaded twice, which would double-count sales
MIN_ROWS_SHARE = 0.20     # fewer rows than this share of a usual day: probably a partial upload
VOLUME_HISTORY_DAYS = 28
VOLUME_Z_LIMIT = 4.0


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: str  # "error" or "warning"
    detail: str
    value: float | None = None


@dataclass
class Report:
    batch_date: str
    rows: int
    results: list[CheckResult] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(not r.passed and r.severity == "error" for r in self.results):
            return "rejected"
        if any(not r.passed for r in self.results):
            return "passed_with_warnings"
        return "passed"

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    def to_dict(self) -> dict:
        return {"batch_date": self.batch_date, "rows": self.rows, "status": self.status,
                "results": [asdict(r) for r in self.results]}


def check_schema(df: pd.DataFrame) -> CheckResult:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    ok = not missing and not extra
    detail = "columns as expected" if ok else f"schema drift: missing {missing}, unexpected {extra}"
    return CheckResult("schema", ok, "error", detail)


def check_not_empty(df: pd.DataFrame) -> CheckResult:
    return CheckResult("not_empty", len(df) > 0, "error", f"{len(df)} rows", float(len(df)))


def check_null_rates(df: pd.DataFrame) -> CheckResult:
    over = {}
    for col, limit in MAX_NULL_RATE.items():
        rate = float(df[col].isna().mean())
        if rate > limit:
            over[col] = round(rate, 3)
    detail = "null rates within limits" if not over else f"too many empty values: {over}"
    return CheckResult("null_rates", not over, "error", detail, max(over.values(), default=0.0))


def check_types(df: pd.DataFrame) -> CheckResult:
    """Every Quantity, Price and InvoiceDate value must parse."""
    bad = {}
    for col, parse in (("Quantity", pd.to_numeric), ("Price", pd.to_numeric), ("InvoiceDate", pd.to_datetime)):
        parsed = parse(df[col], errors="coerce")
        n_bad = int((parsed.isna() & df[col].notna()).sum())
        if n_bad:
            bad[col] = n_bad
    return CheckResult("types", not bad, "error", "all values parse" if not bad else f"unparseable values: {bad}")


def check_values(df: pd.DataFrame, day: date) -> CheckResult:
    """Business rules: no negative prices, no zero quantities, every row dated on the batch day."""
    qty, price = pd.to_numeric(df["Quantity"], errors="coerce"), pd.to_numeric(df["Price"], errors="coerce")
    when = pd.to_datetime(df["InvoiceDate"], errors="coerce").dt.date
    problems = {"negative_price": int((price < 0).sum()), "zero_quantity": int((qty == 0).sum()),
                "wrong_date": int((when.notna() & (when != day)).sum())}
    problems = {k: v for k, v in problems.items() if v}
    return CheckResult("values", not problems, "error", "business rules hold" if not problems else str(problems))


def check_duplicates(df: pd.DataFrame) -> CheckResult:
    rate = float(df.duplicated().mean()) if len(df) else 0.0
    severity = "error" if rate > DUPLICATE_ERROR else "warning"
    return CheckResult("duplicates", rate <= DUPLICATE_WARN, severity,
                       f"{rate:.1%} duplicate rows "
                       f"(warn above {DUPLICATE_WARN:.1%}, reject above {DUPLICATE_ERROR:.0%})",
                       rate)


def check_completeness(df: pd.DataFrame, history: list[dict]) -> CheckResult:
    """Reject a batch with far fewer rows than a usual day (a partial or truncated upload)."""
    recent = history[-VOLUME_HISTORY_DAYS:]
    if len(recent) < 7:
        return CheckResult("completeness", True, "error", f"only {len(recent)} days of history; check skipped")
    usual = float(np.median([h["rows"] for h in recent]))
    share = len(df) / usual
    return CheckResult("completeness", share >= MIN_ROWS_SHARE, "error",
                       f"{len(df)} rows = {share:.0%} of a usual day ({usual:.0f}); "
                       f"minimum {MIN_ROWS_SHARE:.0%}", share)


def check_volume(df: pd.DataFrame, history: list[dict]) -> CheckResult:
    """Row count and sales total against the last 28 accepted batches (robust z-score with median and MAD)."""
    recent = history[-VOLUME_HISTORY_DAYS:]
    if len(recent) < 7:
        return CheckResult("volume", True, "warning", f"only {len(recent)} days of history; check skipped")
    amount = float((pd.to_numeric(df["Quantity"], errors="coerce") * pd.to_numeric(df["Price"], errors="coerce")).sum())
    z = {"rows": _robust_z(len(df), [h["rows"] for h in recent]),
         "sales": _robust_z(amount, [h["sales"] for h in recent])}
    worst = max(z, key=lambda k: abs(z[k]))
    ok = abs(z[worst]) <= VOLUME_Z_LIMIT
    return CheckResult("volume", ok, "warning",
                       f"rows z={z['rows']:+.1f}, sales z={z['sales']:+.1f} (limit ±{VOLUME_Z_LIMIT:.0f})", z[worst])


def _robust_z(x: float, past: list[float]) -> float:
    med = float(np.median(past))
    mad = float(np.median(np.abs(np.array(past) - med))) * 1.4826 or 1.0
    return (x - med) / mad


def run_checks(df: pd.DataFrame, day: date, history: list[dict]) -> Report:
    report = Report(batch_date=day.isoformat(), rows=len(df))
    schema = check_schema(df)
    report.results.append(schema)
    if not schema.passed:          # the other checks need the expected columns
        return report
    report.results.append(check_not_empty(df))
    if len(df) == 0:
        return report
    report.results += [check_null_rates(df), check_types(df), check_values(df, day), check_duplicates(df),
                       check_completeness(df, history), check_volume(df, history)]
    return report


def batch_stats(df: pd.DataFrame) -> dict:
    """What we remember about an accepted batch, for future volume checks."""
    amount = pd.to_numeric(df["Quantity"], errors="coerce") * pd.to_numeric(df["Price"], errors="coerce")
    return {"rows": len(df), "sales": float(amount.sum())}
