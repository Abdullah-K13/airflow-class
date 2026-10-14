#!/bin/bash
# Installs Airflow once, when the codespace is first created.
# Safe to re-run: if Airflow is already there, it skips the install.
set -euo pipefail

AIRFLOW_VERSION="3.3.2"
PYTHON_VERSION="3.12"

echo "--> python: $(command -v python) ($(python -V 2>&1))"

if ! command -v airflow >/dev/null 2>&1; then
  echo "--> installing Apache Airflow $AIRFLOW_VERSION (takes a few minutes)"
  # Installed into the image's own Python, NOT a venv. A venv would need its
  # bin/ added to PATH, and doing that from devcontainer.json is what broke
  # this container the first time round. The container is already an isolated
  # machine - a venv inside it buys nothing and costs a footgun.
  #
  # The constraints file pins every dependency to a combination Airflow was
  # actually tested with. Without it, pip resolves something subtly broken and
  # you find out three weeks later. Never install Airflow without it.
  pip install --no-cache-dir --upgrade pip
  pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
else
  echo "--> airflow already installed, skipping"
fi

# Build the metadata database now, so the first `airflow standalone` in class
# starts in seconds instead of migrating while 40 people watch.
echo "--> preparing the Airflow database at ${AIRFLOW_HOME}"
airflow db migrate

echo
echo "================================================================"
echo " Ready. In the terminal, run:    airflow standalone"
echo " Then open port 8080 from the PORTS tab."
echo " Username: admin   Password: printed in the startup logs"
echo "================================================================"
