#!/bin/bash

sudo apt update -y
sudo apt install -y libmysqlclient-dev libssl-dev comerr-dev libsasl2-dev

python -m venv .venv

source .venv/bin/activate

export AIRFLOW_HOME="$PWD/airflow"
export SLUGIFY_USES_TEXT_UNIDECODE=yes

echo $PWD
pip install wheel
pip install -r requirements.txt