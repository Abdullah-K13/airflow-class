# Airflow 3 — class environment

Everything runs in the cloud. You need a browser and a GitHub account. Nothing
to install on the lab PC.

## Start (students)

1. On this repo: **Code → Codespaces → Create codespace on main**.
2. Wait. The first build installs Airflow and takes a few minutes. You'll see
   `Ready. In the terminal, run: airflow standalone` when it's done.
3. In the VS Code terminal:
   ```bash
   airflow standalone
   ```
4. A toast appears offering port **8080** — click **Open in Browser**. If you
   miss it, use the **PORTS** tab at the bottom.
5. Log in as `admin`. The password is printed in the terminal when Airflow
   starts, and is also in `.airflow/simple_auth_manager_passwords.json.generated`.

Write your DAGs in `dags/`. They show up in the UI within about 5 seconds.

**Stop with `Ctrl+C`.** If you leave the codespace idle it shuts down to save
your free hours — reopen it and run `airflow standalone` again. Your files and
your Airflow database are still there.

## What's in `dags/`

| File | What it teaches |
|---|---|
| `hello_airflow.py` | The smallest possible DAG |
| `simple_math.py` | Tasks passing values with XCom |
| `orders_pipeline.py` | Retries, failures, trigger rules, callbacks |
| `retail_etl_pipeline.py` | A full pipeline: extract with a fallback, validate, quarantine, clean, load idempotently, quality-gate, branch, archive, clean up |

Start with `hello_airflow.py`, finish with `retail_etl_pipeline.py` — read its
docstring at the top of the UI's DAG Docs panel, it explains every stage.

## Notes for the instructor

- **Pinned versions.** Airflow `3.3.2` on Python `3.12`, installed against the
  official constraints file. Change both in `.devcontainer/setup.sh` together —
  the constraints URL contains each.
- **Isolation.** Each student gets their own machine, scheduler and database.
  One broken scheduler affects one student.
- **Cost.** Codespaces free tier is per-account hours, not per-repo. Tell
  students to stop their codespace when the class ends
  (`github.com/codespaces` → **...** → Stop). Idle timeout stops it anyway.
- **No Docker here.** This runs `airflow standalone` directly: one process,
  SQLite, LocalExecutor. Every concept in these DAGs works on it. Teach
  `docker compose` separately when you get to executors and Celery.
- **Config lives in `devcontainer.json`**, as environment variables
  (`AIRFLOW__CORE__DAGS_FOLDER`, `AIRFLOW__CORE__LOAD_EXAMPLES`, …). That's the
  same override mechanism as `airflow.cfg`, and worth showing students:
  `AIRFLOW__<SECTION>__<KEY>`.
- **To reset a student's Airflow** without losing their DAGs:
  `rm -rf .airflow && airflow db migrate`.
