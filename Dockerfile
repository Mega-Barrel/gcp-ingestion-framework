
FROM astrocrpublic.azurecr.io/runtime:3.1-13

COPY src/ /usr/local/airflow/src/

ENV AIRFLOW__CORE__DAGS_FOLDER=/usr/local/airflow/src/astro_dags/dags
ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/src"
