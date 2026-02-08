from __future__ import annotations

import csv
import os
from pathlib import Path
from datetime import timedelta, date

from airflow.sdk import dag, task, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from massive import RESTClient


SYMBOL = "NVDA"

BOOTSTRAP_START = "2025-01-01"
BOOTSTRAP_LAST_DAY = "2025-02-01"
BOOTSTRAP_END_EXCL = "2025-02-02"

VAR_LAST_DATE = "massive_nvda_last_date"
OUT_FILE = Path("/opt/airflow/data/massive/nvda/NVDA_aggs.csv")


def _as_dict(obj) -> dict:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return {"value": str(obj)}


def _read_csv_header(path: Path) -> list[str] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return None


def _write_new_csv(client: RESTClient, symbol: str, start: str, end_excl: str) -> int:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    it = iter(client.list_aggs(symbol, 1, "minute", start, end_excl, limit=50000))
    first = next(it, None)

    with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
        if first is None:
            print(f"[CSV] No data in bootstrap range {start} -> {end_excl}. Created empty {OUT_FILE}")
            return 0

        row1 = _as_dict(first)
        fieldnames = list(row1.keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        writer.writerow(row1)
        count = 1

        for a in it:  # <-- IMPORTANT: continue same iterator (no second API call)
            writer.writerow(_as_dict(a))
            count += 1

    print(f"[CSV] Created {OUT_FILE} with {count} rows (range {start} -> {end_excl})")
    return count


def _append_day_to_csv(client: RESTClient, symbol: str, day: str) -> int:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    d0 = date.fromisoformat(day)
    start = d0.isoformat()
    end_excl = (d0 + timedelta(days=1)).isoformat()

    it = iter(client.list_aggs(symbol, 1, "minute", start, end_excl, limit=50000))
    first = next(it, None)
    if first is None:
        print(f"[CSV] No data for {symbol} on {day}. Nothing appended.")
        return 0

    header = _read_csv_header(OUT_FILE)
    row1 = _as_dict(first)

    # If file missing/empty, create with header
    if header is None:
        with OUT_FILE.open("w", newline="", encoding="utf-8") as f:
            fieldnames = list(row1.keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerow(row1)
            count = 1

            for a in it:
                writer.writerow(_as_dict(a))
                count += 1

        print(f"[CSV] File was missing/empty. Created {OUT_FILE} and wrote {count} rows for {day}")
        return count

    # Otherwise append
    with OUT_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        writer.writerow(row1)
        count = 1

        for a in it:
            writer.writerow(_as_dict(a))
            count += 1

    print(f"[CSV] Appended {count} rows for {symbol} on {day} -> {OUT_FILE}")
    return count


@dag(
    dag_id="massive_nvda_daily",
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["massive", "stocks"],
)
def massive_daily():

    @task
    def fetch_nvda_aggs_to_one_csv():
        api_key = os.environ.get("MASSIVE_API_KEY")
        client = RESTClient(api_key) if api_key else RESTClient()

        last_date = Variable.get(VAR_LAST_DATE, default=None)

        if last_date is None:
            total = _write_new_csv(client, SYMBOL, BOOTSTRAP_START, BOOTSTRAP_END_EXCL)
            Variable.set(VAR_LAST_DATE, BOOTSTRAP_LAST_DAY)
            print(f"[BOOTSTRAP] Set {VAR_LAST_DATE}={BOOTSTRAP_LAST_DAY}. Total rows: {total}")
            return total

        next_day = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
        added = _append_day_to_csv(client, SYMBOL, next_day)
        Variable.set(VAR_LAST_DATE, next_day)
        print(f"[DAILY] Updated {VAR_LAST_DATE}={next_day}. Rows added: {added}")
        return added

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_nvda_csv_to_market_postgres",
        trigger_dag_id="load_nvda_csv_to_market_postgres",
        wait_for_completion=False,
        trigger_rule="all_success",
    )

    fetch_task = fetch_nvda_aggs_to_one_csv()
    fetch_task >> trigger_load


massive_daily()
