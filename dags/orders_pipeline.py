"""Orders pipeline — the DAG for teaching retries and failures.

A small, realistic shape: fetch some orders, then fan out into four branches
that each demonstrate one thing a class needs to see.

    fetch_orders ─┬─ count_orders ──┐
                  │                 ├─ build_summary ─ email_summary ─┐
                  ├─ total_revenue ─┘                                 │
                  │                                                   ├─ cleanup
                  ├─ check_quality  (fails twice, then passes)        │
                  │                                                   │
                  └─ upload_backup  (fails for good) ─ confirm_backup ┘
                                                        (never runs)

What each branch is for:

  * count_orders / total_revenue   parallel tasks, then build_summary waits
                                   for BOTH before it starts.
  * check_quality                  fails on attempts 1 and 2, succeeds on
                                   attempt 3. This is retries doing their job:
                                   the task ends GREEN with two failed
                                   attempts behind it.
  * upload_backup                  fails on every attempt. Retries run out and
                                   it ends RED.
  * confirm_backup                 never runs at all, because its upstream
                                   failed. State: upstream_failed.
  * cleanup                        runs ANYWAY, because of its trigger rule.

The failures are deterministic, not random, so the demo is the same every
time you run it. They are driven by the attempt number, which Airflow gives
each task as `try_number`.
"""

from airflow.sdk import dag, task, get_current_context
import pendulum

# Change these in front of the class and re-trigger to show re-parsing.
ORDER_COUNT = 6
QUALITY_PASSES_ON_ATTEMPT = 3


def announce_retry(context):
    """Runs each time a task is about to be retried."""
    ti = context["task_instance"]
    print(f"[RETRY CALLBACK] {ti.task_id} failed attempt {ti.try_number}, will try again")


def announce_failure(context):
    """Runs once, when a task has given up for good."""
    ti = context["task_instance"]
    print(f"[FAILURE CALLBACK] {ti.task_id} has failed for good. Someone should be told.")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "teaching", "retries"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=10),
        "on_retry_callback": announce_retry,
        "on_failure_callback": announce_failure,
    },
    doc_md=__doc__,
)
def orders_pipeline():

    @task
    def fetch_orders() -> list[dict]:
        """Pretend we called an API. Returns a small list of orders."""
        orders = [
            {"id": i, "amount": 100 * i, "city": "Karachi" if i % 2 else "Lahore"}
            for i in range(1, ORDER_COUNT + 1)
        ]
        print(f"fetched {len(orders)} orders")
        return orders

    # ---- branch 1: two tasks in parallel, then one that waits for both ----

    @task
    def count_orders(orders: list[dict]) -> int:
        print(f"counting {len(orders)} orders")
        return len(orders)

    @task
    def total_revenue(orders: list[dict]) -> int:
        total = sum(o["amount"] for o in orders)
        print(f"total revenue is {total}")
        return total

    @task
    def build_summary(count: int, revenue: int) -> str:
        """Only starts once count_orders AND total_revenue have both finished."""
        average = revenue / count
        line = f"{count} orders, {revenue} total, {average:.0f} average"
        print(line)
        return line

    @task
    def email_summary(summary: str) -> None:
        print(f"[EMAIL] Good morning. Yesterday: {summary}")

    # ---- branch 2: fails, retries, and then succeeds ----

    @task(retries=3, retry_delay=pendulum.duration(seconds=15))
    def check_quality(orders: list[dict]) -> str:
        """Fails the first two attempts on purpose, then passes.

        This is what a flaky network call looks like. Airflow retries it and
        the task ends up green, so the pipeline is not ruined by a blip.
        """
        attempt = get_current_context()["ti"].try_number
        print(f"quality check: attempt {attempt}")

        if attempt < QUALITY_PASSES_ON_ATTEMPT:
            raise ValueError(
                f"could not reach the quality service (attempt {attempt}) - this one is on purpose"
            )

        bad = [o for o in orders if o["amount"] <= 0]
        print(f"quality check passed on attempt {attempt}, {len(bad)} bad rows")
        return "passed"

    # ---- branch 3: fails every time, and blocks what comes after it ----

    @task(retries=2, retry_delay=pendulum.duration(seconds=10))
    def upload_backup(orders: list[dict]) -> str:
        """Always fails. Shows what a task looks like when retries run out."""
        attempt = get_current_context()["ti"].try_number
        print(f"uploading {len(orders)} orders to the backup server: attempt {attempt}")
        raise ConnectionError("backup server is not reachable - this one is on purpose")

    @task
    def confirm_backup(result: str) -> None:
        """Never runs. upload_backup failed, so this is marked upstream_failed."""
        print(f"backup confirmed: {result}")

    # ---- the one that runs no matter what ----

    @task(trigger_rule="all_done", retries=0)
    def cleanup() -> None:
        """`all_done` means: start once everything upstream has finished,
        whether it succeeded, failed or was skipped. Use this for tidying up."""
        print("[CLEANUP] deleting temporary files - this runs even though a task failed")

    # ---- wiring ----

    orders = fetch_orders()

    summary = build_summary(count_orders(orders), total_revenue(orders))
    emailed = email_summary(summary)

    quality = check_quality(orders)
    backup_confirmed = confirm_backup(upload_backup(orders))

    [emailed, quality, backup_confirmed] >> cleanup()


orders_pipeline()
