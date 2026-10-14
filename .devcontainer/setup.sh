#!/bin/bash
# Installs Airflow once, when the codespace is first created.
# Safe to re-run: if the venv is already there, it does nothing.
set -euo pipefail

AIRFLOW_VERSION="3.3.2"
PYTHON_VERSION="3.12"
VENV="/workspaces/airflow-venv"

# The venv lives OUTSIDE the repo folder, so it is never committed, but still
# on the persistent /workspaces volume, so it survives stop/start and rebuilds.
if [ ! -x "$VENV/bin/airflow" ]; then
  echo "--> creating venv at $VENV"
  python -m venv "$VENV"

  echo "--> installing Apache Airflow $AIRFLOW_VERSION (this takes a few minutes)"
  # The constraints file pins every dependency to a combination Airflow was
  # actually tested with. Without it pip resolves something subtly broken and
  # you find out three weeks later. Never install Airflow without it.
  "$VENV/bin/pip" install --upgrade pip
  "$VENV/bin/pip" install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
else
  echo "--> airflow already installed, skipping"
fi

# Build the metadata database now, so the first `airflow standalone` in class
# starts in seconds instead of migrating while 40 people watch.
echo "--> preparing the Airflow database at ${AIRFLOW_HOME}"
"$VENV/bin/airflow" db migrate

echo
echo "================================================================"
echo " Ready. In the terminal, run:    airflow standalone"
echo " Then open port 8080 from the PORTS tab."
echo " Username: admin   Password: printed in the startup logs"
echo "================================================================"
