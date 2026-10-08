from datetime import date

import numpy as np
import pandas as pd
from conftest import history

from txpipe.checks import batch_stats, run_checks

DAY = date(2011, 9, 1)


def failed(report):
    return {r.name: r.severity for r in report.failures}


def test_a_good_batch_passes(batch):
    df = batch(DAY)
    report = run_checks(df, DAY, history(sales=batch_stats(df)["sales"]))
    assert report.status == "passed", report.failures


def test_schema_drift_is_rejected(batch):
    report = run_checks(batch(DAY).rename(columns={"Price": "UnitPrice"}), DAY, [])
    assert report.status == "rejected" and failed(report) == {"schema": "error"}
    assert "UnitPrice" in report.results[0].detail


def test_empty_file_is_rejected(batch):
    assert run_checks(batch(DAY).iloc[0:0], DAY, []).status == "rejected"


def test_missing_key_values_are_rejected(batch):
    df = batch(DAY)
    df.loc[3, "Quantity"] = np.nan
    assert failed(run_checks(df, DAY, [])) == {"null_rates": "error"}


def test_guest_orders_without_customer_id_are_allowed(batch):
    df = batch(DAY)
    df.loc[: len(df) // 2, "Customer ID"] = np.nan     # about half: normal on some real days
    assert run_checks(df, DAY, []).status == "passed"


def test_unparseable_values_are_rejected(batch):
    df = batch(DAY).astype({"Price": object})
    df.loc[0, "Price"] = "abc"
    assert "types" in failed(run_checks(df, DAY, []))


def test_business_rules(batch):
    df = batch(DAY)
    df.loc[0, "Price"] = -1.0
    df.loc[1, "Quantity"] = 0
    df.loc[2, "InvoiceDate"] = "2011-08-31 10:00:00"
    values = next(r for r in run_checks(df, DAY, []).results if r.name == "values")
    assert not values.passed and all(k in values.detail for k in ("negative_price", "zero_quantity", "wrong_date"))


def test_a_few_duplicates_warn_but_a_doubled_file_is_rejected(batch):
    df = batch(DAY)
    few = pd.concat([df, df.head(4)])                   # ~6% duplicates
    assert failed(run_checks(few, DAY, [])) == {"duplicates": "warning"}
    assert run_checks(pd.concat([df, df]), DAY, []).status == "rejected"


def test_partial_upload_is_rejected(batch):
    report = run_checks(batch(DAY, rows=8), DAY, history(rows=60))
    assert failed(report).get("completeness") == "error"


def test_unusual_volume_only_warns(batch):
    report = run_checks(batch(DAY, rows=200), DAY, history(rows=60))
    assert report.status == "passed_with_warnings" and failed(report) == {"volume": "warning"}


def test_report_serialises(batch):
    d = run_checks(batch(DAY), DAY, []).to_dict()
    assert d["status"] == "passed" and d["batch_date"] == "2011-09-01" and len(d["results"]) == 8
