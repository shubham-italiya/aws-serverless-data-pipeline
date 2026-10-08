# Demo run: 59 files through the pipeline

Every file was uploaded to `raw/` in a fake AWS account (moto) and processed by the real Lambda handler;
the Glue job code then curated the accepted days into Parquet.

## Outcome by input

| Input | passed | passed_with_warnings | rejected |
|---|---|---|---|
| file sent twice | 0 | 0 | 1 |
| missing quantities | 0 | 0 | 1 |
| negative prices | 0 | 0 | 1 |
| partial upload | 0 | 0 | 1 |
| previous day mixed in | 0 | 0 | 1 |
| real day | 49 | 3 | 0 |
| schema drift | 0 | 0 | 1 |
| unreadable | 0 | 0 | 1 |

SNS alerts sent: **10**. Days curated to Parquet: **52**.

## Every batch that was not a clean pass

| Date | Input | Rows | Status | Why |
|---|---|---|---|---|
| 2011-09-15 | real day | 2,197 | passed_with_warnings | volume: rows z=+1.2, sales z=+4.2 (limit ±4) |
| 2011-09-20 | real day | 1,718 | passed_with_warnings | volume: rows z=+0.3, sales z=+9.1 (limit ±4) |
| 2011-09-21 | real day | 3,129 | passed_with_warnings | volume: rows z=+4.4, sales z=+0.8 (limit ±4) |
| 2011-11-01 | schema drift | 1,786 | rejected | schema: schema drift: missing ['Price'], unexpected ['UnitPrice'] |
| 2011-11-02 | missing quantities | 2,632 | rejected | null_rates: too many empty values: {'Quantity': 0.05} |
| 2011-11-03 | negative prices | 2,419 | rejected | values: {'negative_price': 3} |
| 2011-11-04 | partial upload | 120 | rejected | completeness: 120 rows = 5% of a usual day (2392); minimum 20%; volume: rows z=-3.0, sales z=-4.1 (limit ±4) |
| 2011-11-06 | file sent twice | 6,874 | rejected | duplicates: 51.2% duplicate rows (warn above 3.5%, reject above 20%); volume: rows z=+5.9, sales z=+4.8 (limit ±4) |
| 2011-11-07 | previous day mixed in | 2,149 | rejected | values: {'wrong_date': 50} |
| 2011-11-08 | unreadable | 0 | rejected | readable: cannot read CSV: 'utf-8' codec can't decode byte 0x82 in position 1: invalid start byte |

## Example alert (SNS message)

```
[transactions] REJECTED: batch 2011-11-01

Batch 2011-11-01: rejected (1786 rows)

- schema (error): schema drift: missing ['Price'], unexpected ['UnitPrice']

Full report: s3://demo-transactions/reports/date=2011-11-01/report.json
```
