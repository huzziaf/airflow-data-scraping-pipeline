# Airflow Data Extraction Pipeline
This project automates a data extraction lifecycle from extracting data, transforming it, saving it to versioning the data with dvc and code with git.

## Follow these steps:
- Open WSL or Linux
- Navigate to the directory of repo
- Run `pip install apache-airflow` to install airflow
- Run `export AIRFLOW_HOME="path of the repo"`
- Run `airflow db init` to initialize airflow database
- Run `airflow scheduler -D`
- Run `airflow webserver -p 8080` and open airflow user interface at `http://localhost:8080`
- Find the dag with the name specified in the code and activate the dag
- Click play button on top right to manually trigger the dag run

## Points To Note
- Place all/any dags in the /dags folder for airflow to detect
- Run `airflow dags list` to check if airflow properly picks up dags from the dagbag (dag folder which contains all dags)
