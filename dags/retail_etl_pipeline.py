"""Retail ETL - the full pipeline, one teaching DAG.

This is the shape a real data pipeline has: get the data, land it untouched,
check it, clean it, load it, prove it is good, publish it, tidy up. Every
stage below is a stage you will find in production somewhere.

    prepare_run
         |
         +- extract_from_api --(fails)--> extract_from_seed   FALLBACK
         |         |      (all_failed)              |
         |         +-----------(ok)-----------------+
         |                     |
    land_raw <-----------------+   (one_success: takes whichever path lived)
         |
    wait_for_raw_file          SENSOR: pokes until the file is really there
         |
    validate_rows              good rows go on, bad rows go to quarantine
         |
    clean_rows                 dedupe, trim, cast, normalise, derive
         |
    load_to_warehouse          idempotent: same run twice = same table
         |
    run_quality_checks         counts, nulls, negatives, bad-row ratio
         |
    decide_publish             BRANCH: one of the two below runs
         +- publish_marts      checks passed
         +- raise_alert        checks failed (the DAG still ends cleanly)
         |
    archive_raw                keeps the raw file, moves it out of the way
         |
    cleanup                    ALWAYS runs (all_done), even after a failure

The five lessons hiding in here
-------------------------------
1. FALLBACKS.      extract_from_api may fail. extract_from_seed has
                   trigger_rule="all_failed" AND sits downstream of the API
                   task, so it only wakes up when the API path died. land_raw
                   joins them with trigger_rule="one_success", so it takes
                   whichever path survived. This is THE Airflow fallback
                   pattern - learn these three facts together.
2. RAW IS SACRED.  land_raw writes the payload to disk exactly as it arrived
                   and never edits it. Every later stage reads a copy. When
                   something looks wrong in six months, the raw file is the
                   only evidence you have.
3. SMALL XCOMS.    Tasks pass FILE PATHS to each other, not the data itself.
                   XCom is a message bus in a database, not a disk. Pushing a
                   million rows through it is how you kill a scheduler.
4. IDEMPOTENCY.    load_to_warehouse deletes this run's batch before it
                   inserts it. Re-run the DAG as many times as you like, the
                   warehouse ends up the same. A pipeline you cannot safely
                   re-run is a pipeline you cannot fix.
5. BAD DATA LIVES. Rows that fail validation are not dropped and are not
                   allowed to crash the run. They are written to quarantine
                   so a human can look at them tomorrow.

Things to try in class
----------------------
* Trigger with `force_api_failure: true` in the config box - watch the
  fallback branch light up and the pipeline survive.
* Trigger with `max_bad_row_pct: 0` - watch the branch go to raise_alert.
* Trigger it twice with the same settings - watch the warehouse row count
  stay the same. That is idempotency.
* Everything it writes lands in `dags/data/` on your laptop. Go and open it.
"""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pendulum
from airflow.sdk import Param, Variable, dag, get_current_context, task, task_group

# --------------------------------------------------------------------------
# Where things live. One folder per stage, which is also how you would lay
# out a bucket in S3 or GCS: raw / quarantine / clean / marts / archive.
# --------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
QUARANTINE_DIR = DATA_DIR / "quarantine"
CLEAN_DIR = DATA_DIR / "clean"
MARTS_DIR = DATA_DIR / "marts"
ARCHIVE_DIR = DATA_DIR / "archive"
WAREHOUSE = DATA_DIR / "warehouse.db"

VALID_CITIES = {"karachi", "lahore", "islamabad", "multan", "peshawar"}


# --------------------------------------------------------------------------
# Plain Python helpers. No Airflow in here on purpose: logic you can unit
# test without a scheduler is logic you can trust.
# --------------------------------------------------------------------------


def batch_id() -> str:
    """A safe, stable name for this run. Used in filenames and in the table.

    run_id looks like `manual__2026-10-14T12:00:00+00:00`, which is not a
    legal filename on Windows. Strip it down to letters and numbers.
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", get_current_context()["run_id"]).strip("_")


def seed_orders(count: int) -> list[dict]:
    """The offline copy of the data, for when the API is unreachable.

    A fallback source is only useful if it is deliberately boring and always
    available. This one is generated, so the class demo never depends on wifi.
    """
    cities = ["Karachi", "Lahore", "Islamabad", "Multan", "Peshawar"]
    return [
        {
            "order_id": i,
            "customer_id": 1000 + (i % 7),
            "amount": round(250.0 + i * 37.5, 2),
            "items": 1 + (i % 5),
            "city": cities[i % len(cities)],
            "ordered_at": f"2026-10-{1 + (i % 28):02d}",
        }
        for i in range(1, count + 1)
    ]


def legacy_rows() -> list[dict]:
    """Deliberately broken rows, as if exported from an old system.

    Real sources are dirty. If your demo data is perfect, your students never
    see validation or cleaning do anything. These are the classics:
    a duplicate, a missing id, a negative amount, untrimmed mixed-case text,
    an amount that arrived as a string, and a junk date.
    """
    return [
        {"order_id": 1, "customer_id": 1001, "amount": 287.5, "items": 2,
         "city": "Karachi", "ordered_at": "2026-10-02"},
        {"order_id": None, "customer_id": 1002, "amount": 400.0, "items": 1,
         "city": "Lahore", "ordered_at": "2026-10-03"},
        {"order_id": 9001, "customer_id": 1003, "amount": -50.0, "items": 1,
         "city": "Multan", "ordered_at": "2026-10-04"},
        {"order_id": 9002, "customer_id": 1004, "amount": "1,250.00", "items": "3",
         "city": "  lahore  ", "ordered_at": "2026-10-05"},
        {"order_id": 9003, "customer_id": None, "amount": 310.0, "items": 2,
         "city": "Quetta", "ordered_at": "not-a-date"},
    ]


def map_api_payload(payload: dict) -> list[dict]:
    """Turn someone else's JSON into OUR column names.

    Do this once, at the edge, in one function. The alternative is every
    downstream task knowing what `totalQuantity` means, and that is how a
    pipeline rots.
    """
    return [
        {
            "order_id": cart["id"],
            "customer_id": cart["userId"],
            "amount": cart["total"],
            "items": cart["totalQuantity"],
            "city": "Karachi",
            "ordered_at": "2026-10-01",
        }
        for cart in payload.get("carts", [])
    ]


def to_float(value) -> float | None:
    """`"1,250.00"` -> 1250.0, junk -> None. Never raises."""
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def to_int(value) -> int | None:
    number = to_float(value)
    return int(number) if number is not None else None


def row_errors(row: dict) -> list[str]:
    """Every reason this row is unusable. Collect them ALL, don't stop at
    the first: a human reading the quarantine file wants the whole story."""
    errors = []
    if to_int(row.get("order_id")) is None:
        errors.append("missing_or_bad_order_id")
    if to_int(row.get("customer_id")) is None:
        errors.append("missing_customer_id")
    amount = to_float(row.get("amount"))
    if amount is None:
        errors.append("bad_amount")
    elif amount <= 0:
        errors.append("non_positive_amount")
    try:
        datetime.strptime(str(row.get("ordered_at", "")).strip(), "%Y-%m-%d")
    except ValueError:
        errors.append("bad_ordered_at")
    return errors


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: str | Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


# --------------------------------------------------------------------------
# Callbacks: Airflow telling you something happened, without you watching.
# --------------------------------------------------------------------------


def on_retry(context) -> None:
    ti = context["task_instance"]
    print(f"[RETRY] {ti.task_id} failed attempt {ti.try_number} - trying again")


def on_failure(context) -> None:
    ti = context["task_instance"]
    print(f"[ALERT] {ti.task_id} has failed for good. In production this pages someone.")


@dag(
    dag_id="retail_etl_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["teaching", "etl", "fallbacks", "data-quality"],
    doc_md=__doc__,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(seconds=10),
        "retry_exponential_backoff": True,
        "on_retry_callback": on_retry,
        "on_failure_callback": on_failure,
    },
    params={
        "source_url": Param(
            "https://dummyjson.com/carts",
            type="string",
            description="The API we try first. Any unreachable URL will demo the fallback.",
        ),
        "force_api_failure": Param(
            False,
            type="boolean",
            description="Tick this to make the API step fail on purpose.",
        ),
        "seed_count": Param(12, type="integer", minimum=1, maximum=500),
        "max_bad_row_pct": Param(
            40,
            type="integer",
            minimum=0,
            maximum=100,
            description="How much rubbish we tolerate before the quality gate fails.",
        ),
    },
)
def retail_etl_pipeline():

    # ---------------------------------------------------------------- setup

    @task
    def prepare_run() -> str:
        """Make the folders and print the run's context.

        `get_current_context()` is how a plain Python task gets at the run
        metadata: which day it is processing, which attempt this is, what the
        user typed into the config box. Read the log of this task first.
        """
        context = get_current_context()
        for folder in (RAW_DIR, QUARANTINE_DIR, CLEAN_DIR, MARTS_DIR, ARCHIVE_DIR):
            folder.mkdir(parents=True, exist_ok=True)

        batch = batch_id()
        print(f"run_id         : {context['run_id']}")
        print(f"logical date   : {context['logical_date']}")
        print(f"data interval  : {context['data_interval_start']} -> {context['data_interval_end']}")
        print(f"params         : {dict(context['params'])}")
        print(f"batch_id       : {batch}")

        # A Variable is cluster-wide config, set in the UI under Admin.
        # Wrapped, because a demo must not break just because nobody set it.
        try:
            owner = Variable.get("retail_pipeline_owner")
        except Exception:
            owner = "data-team (default - set the Variable to change this)"
        print(f"owned by       : {owner}")
        return batch

    # ---------------------------------------------- EXTRACT (with fallback)

    @task_group(group_id="extract")
    def extract(prepared):
        """Two ways to get the same data. The second only runs if the first dies."""

        @task(retries=2, retry_delay=pendulum.duration(seconds=5))
        def extract_from_api() -> list[dict]:
            """Preferred source. Allowed to fail - that is the whole point.

            `requests` is imported INSIDE the task on purpose. A missing
            library then fails this one task and hands over to the fallback,
            instead of breaking the DAG file for everybody.
            """
            context = get_current_context()
            params = context["params"]

            if params["force_api_failure"]:
                raise RuntimeError("forced API failure - you ticked the box")

            import requests

            url = params["source_url"]
            print(f"GET {url} (attempt {context['ti'].try_number})")
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            orders = map_api_payload(response.json())
            if not orders:
                raise ValueError("API answered, but with zero orders - treating that as a failure")
            print(f"got {len(orders)} orders from the API")
            return orders

        @task(trigger_rule="all_failed", retries=0)
        def extract_from_seed() -> list[dict]:
            """THE FALLBACK.

            trigger_rule="all_failed" means: only run if everything upstream
            of me failed. On a good day this task is skipped and you never
            think about it. On a bad day it is the reason you still have data.
            """
            count = get_current_context()["params"]["seed_count"]
            print(f"API path failed - falling back to {count} seeded orders")
            return seed_orders(count)

        @task(trigger_rule="one_success")
        def land_raw(from_api: list[dict] | None, from_seed: list[dict] | None) -> str:
            """THE JOIN, and the raw landing zone.

            trigger_rule="one_success" means: run as soon as ANY parent
            succeeded, and do not care that the other one failed. This is the
            rule a fallback join needs. The obvious-looking
            "none_failed_min_one_success" is a trap here: it refuses to run
            when a parent has FAILED, which is precisely the case we are
            trying to survive.

            Exactly one of my two inputs will hold data; the other is None,
            because that task was skipped or failed.

            Note the honest trade-off: if the API task used up its retries,
            that task stays RED and so does the DAG run - even though the
            data made it through. That is usually what you want (someone
            should go and fix the API). If you would rather the run go green,
            replace extract_from_api with a @task.branch that CHOOSES a
            source instead of failing into one.

            Then: write the payload to disk UNTOUCHED and return only the
            path. Downstream tasks read the file. XCom carries a string.
            """
            orders = from_api if from_api else from_seed
            source = "api" if from_api else "seed"
            if not orders:
                raise ValueError("both the API and the fallback produced nothing")

            # Late arrivals from the old system. Dirty on purpose, so the
            # validation and cleaning stages have real work to do.
            orders = list(orders) + legacy_rows()

            path = RAW_DIR / f"orders_{batch_id()}.json"
            path.write_text(json.dumps({"source": source, "orders": orders}, indent=2), encoding="utf-8")
            print(f"landed {len(orders)} raw rows from '{source}' at {path}")
            return str(path)

        api = extract_from_api()
        seed = extract_from_seed()
        # A task group is not automatically downstream of anything. Chain the
        # group's ROOT task, or prepare_run would race the extract.
        prepared >> api

        # The fallback must sit DOWNSTREAM of the task it is covering for.
        # Without this edge extract_from_seed has no parents at all, and
        # "all_failed" over zero parents is trivially true - it would run on
        # every single run, which is not a fallback, it is a duplicate.
        api >> seed
        return land_raw(api, seed)

    # --------------------------------------------------------------- SENSOR

    @task.sensor(poke_interval=5, timeout=60, mode="reschedule")
    def wait_for_raw_file(raw_path: str) -> bool:
        """A sensor is a task that waits for the world to be ready.

        Returning False means "not yet, ask me again in poke_interval
        seconds". mode="reschedule" frees the worker slot between pokes
        instead of sitting there holding it - always prefer it for anything
        that waits minutes rather than seconds.
        """
        exists = Path(raw_path).exists()
        print(f"is {raw_path} there yet? {exists}")
        return exists

    # ----------------------------------------------------- VALIDATE + CLEAN

    @task_group(group_id="transform")
    def transform(raw_path: str):

        @task
        def validate_rows(raw_path: str) -> dict:
            """Split the data into rows we trust and rows we do not.

            Note what this task does NOT do: throw the bad rows away, or
            crash. Bad rows go to a quarantine file with the reason attached,
            and the run carries on with what is usable.
            """
            payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
            rows = payload["orders"]

            good, bad = [], []
            for row in rows:
                errors = row_errors(row)
                if errors:
                    bad.append({**row, "errors": ";".join(errors)})
                else:
                    good.append(row)

            batch = batch_id()
            good_path = CLEAN_DIR / f"valid_{batch}.csv"
            write_csv(good_path, good,
                      ["order_id", "customer_id", "amount", "items", "city", "ordered_at"])

            bad_path = ""
            if bad:
                bad_path = str(QUARANTINE_DIR / f"rejected_{batch}.csv")
                write_csv(bad_path, bad,
                          ["order_id", "customer_id", "amount", "items", "city", "ordered_at", "errors"])
                for row in bad:
                    print(f"[QUARANTINE] order_id={row.get('order_id')} -> {row['errors']}")

            print(f"{len(good)} rows passed, {len(bad)} rows quarantined")
            return {
                "valid_path": str(good_path),
                "quarantine_path": bad_path,
                "valid_count": len(good),
                "bad_count": len(bad),
                "total": len(rows),
            }

        @task
        def clean_rows(validated: dict) -> str:
            """Make the good rows consistent. This is the unglamorous 80%.

            Five jobs, in order:
              1. cast  - everything out of a CSV is a string
              2. trim and normalise text - "  lahore  " and "Lahore" are one city
              3. flag the unknown - a city we do not recognise becomes 'other',
                 it is not silently kept and not silently dropped
              4. derive - amount_per_item is computed once, here, not in
                 fifteen different dashboards
              5. dedupe - keyed by order_id, then sorted, so the output is
                 byte-identical every run
            """
            rows = read_csv(validated["valid_path"])
            cleaned: dict[int, dict] = {}
            for row in rows:
                city = str(row["city"]).strip().lower()
                items = to_int(row["items"]) or 1
                amount = to_float(row["amount"])
                order_id = to_int(row["order_id"])
                cleaned[order_id] = {
                    "order_id": order_id,
                    "customer_id": to_int(row["customer_id"]),
                    "amount": round(amount, 2),
                    "items": items,
                    "city": city if city in VALID_CITIES else "other",
                    "ordered_at": str(row["ordered_at"]).strip(),
                    "amount_per_item": round(amount / items, 2),
                }

            final = sorted(cleaned.values(), key=lambda r: r["order_id"])
            removed = len(rows) - len(final)
            fields = ["order_id", "customer_id", "amount", "items", "city",
                      "ordered_at", "amount_per_item"]
            path = CLEAN_DIR / f"clean_{batch_id()}.csv"
            write_csv(path, final, fields)
            print(f"cleaned {len(final)} rows, {removed} duplicate(s) collapsed -> {path}")
            return str(path)

        validated = validate_rows(raw_path)
        return validated, clean_rows(validated)

    # ----------------------------------------------------------------- LOAD

    @task
    def load_to_warehouse(clean_path: str) -> int:
        """Write to the warehouse so that running twice changes nothing.

        The pattern: DELETE this batch, then INSERT this batch, in one
        transaction. Re-running a failed day is then completely safe, which
        means you can fix a pipeline at 3am without thinking hard. That is
        worth more than any clever upsert.
        """
        rows = read_csv(clean_path)
        batch = batch_id()
        loaded_at = pendulum.now("UTC").to_iso8601_string()

        with sqlite3.connect(WAREHOUSE) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS orders (
                       order_id        INTEGER,
                       customer_id     INTEGER,
                       amount          REAL,
                       items           INTEGER,
                       city            TEXT,
                       ordered_at      TEXT,
                       amount_per_item REAL,
                       batch_id        TEXT,
                       loaded_at       TEXT,
                       PRIMARY KEY (order_id, batch_id))"""
            )
            deleted = conn.execute("DELETE FROM orders WHERE batch_id = ?", (batch,)).rowcount
            conn.executemany(
                """INSERT INTO orders VALUES
                   (:order_id, :customer_id, :amount, :items, :city,
                    :ordered_at, :amount_per_item, :batch_id, :loaded_at)""",
                [{**row, "batch_id": batch, "loaded_at": loaded_at} for row in rows],
            )

        print(f"deleted {deleted} old row(s) for this batch, inserted {len(rows)} -> {WAREHOUSE}")
        return len(rows)

    # --------------------------------------------------------- QUALITY GATE

    @task
    def run_quality_checks(loaded: int, validated: dict) -> dict:
        """Measure the data, then decide. Never assume the load worked.

        Checks live AFTER the load and BEFORE anyone is told the data is
        ready. A silent wrong number is far more expensive than a loud
        failed pipeline.
        """
        params = get_current_context()["params"]
        batch = batch_id()

        with sqlite3.connect(WAREHOUSE) as conn:
            count, revenue, nulls, negatives = conn.execute(
                """SELECT COUNT(*), COALESCE(SUM(amount), 0),
                          COALESCE(SUM(customer_id IS NULL), 0),
                          COALESCE(SUM(amount <= 0), 0)
                   FROM orders WHERE batch_id = ?""",
                (batch,),
            ).fetchone()

        total = validated["total"] or 1
        bad_pct = round(100 * validated["bad_count"] / total)

        checks = {
            "rows_were_loaded": count > 0,
            "load_matches_clean_file": count == loaded,
            "no_null_customers": not nulls,
            "no_negative_amounts": not negatives,
            "bad_rows_within_tolerance": bad_pct <= params["max_bad_row_pct"],
        }
        for name, passed in checks.items():
            print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
        print(f"rows={count} revenue={revenue:.2f} quarantined={bad_pct}% "
              f"(limit {params['max_bad_row_pct']}%)")

        return {
            "passed": all(checks.values()),
            "rows": count,
            "revenue": round(revenue, 2),
            "bad_pct": bad_pct,
            "failed_checks": [name for name, ok in checks.items() if not ok],
        }

    @task.branch
    def decide_publish(report: dict) -> str:
        """Pick ONE path. Everything down the other path is skipped.

        A branch returns the task_id it wants to run. This is how a pipeline
        reacts to its own data instead of blindly carrying on.
        """
        choice = "publish_marts" if report["passed"] else "raise_alert"
        print(f"quality gate -> {choice}")
        return choice

    @task
    def publish_marts(clean_path: str, report: dict) -> str:
        """The only stage the business actually sees: an aggregate per city."""
        by_city: dict[str, dict] = {}
        for row in read_csv(clean_path):
            city = by_city.setdefault(row["city"], {"city": row["city"], "orders": 0, "revenue": 0.0})
            city["orders"] += 1
            city["revenue"] = round(city["revenue"] + float(row["amount"]), 2)

        rows = sorted(by_city.values(), key=lambda r: -r["revenue"])
        path = MARTS_DIR / f"revenue_by_city_{batch_id()}.csv"
        write_csv(path, rows, ["city", "orders", "revenue"])
        print(f"published {len(rows)} city rows "
              f"({report['rows']} orders, {report['revenue']} revenue) -> {path}")
        return str(path)

    @task
    def raise_alert(report: dict) -> None:
        """Runs instead of publishing when the gate fails.

        It logs loudly but does not raise. The point is to stop bad data
        reaching the business, not to turn the whole DAG red - the data is
        still safely in the warehouse for someone to inspect.
        """
        print(f"[DATA QUALITY ALERT] failed: {', '.join(report['failed_checks'])}")
        print(f"[DATA QUALITY ALERT] batch {batch_id()} was NOT published. "
              f"Check the quarantine folder.")

    # -------------------------------------------------------------- TIDY UP

    @task(trigger_rule="none_failed_min_one_success")
    def archive_raw(raw_path: str) -> None:
        """One of publish/alert was skipped, so this needs the same rule as
        the fallback join: don't wait for a task that was never going to run."""
        source = Path(raw_path)
        if source.exists():
            target = ARCHIVE_DIR / source.name
            source.replace(target)
            print(f"archived raw payload -> {target}")

    @task(trigger_rule="all_done", retries=0)
    def cleanup() -> None:
        """Runs whatever happened upstream: success, failure or skip.

        `all_done` is for anything that must happen regardless - releasing a
        lock, dropping a temp table, deleting scratch files. Here we delete
        the intermediate 'valid_' files but keep clean/, quarantine/ and
        archive/, because those are evidence.
        """
        removed = 0
        for leftover in CLEAN_DIR.glob("valid_*.csv"):
            leftover.unlink()
            removed += 1
        print(f"[CLEANUP] removed {removed} intermediate file(s). "
              f"Raw, clean and quarantine kept.")

    # --------------------------------------------------------------- WIRING

    prepared = prepare_run()
    raw_path = extract(prepared)

    waited = wait_for_raw_file(raw_path)
    validated, clean_path = transform(raw_path)
    waited >> validated

    report = run_quality_checks(load_to_warehouse(clean_path), validated)
    gate = decide_publish(report)

    published = publish_marts(clean_path, report)
    alerted = raise_alert(report)
    gate >> [published, alerted]

    archived = archive_raw(raw_path)
    [published, alerted] >> archived >> cleanup()


retail_etl_pipeline()
