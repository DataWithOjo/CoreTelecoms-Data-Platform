FROM apache/airflow:3.1.3

USER root
RUN apt-get update \
&& apt-get install -y --no-install-recommends \
        vim \
&& apt-get autoremove -yqq --purge \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY --chown=airflow:root dags /opt/airflow/dags

COPY --chown=airflow:root scripts /opt/airflow/scripts

COPY --chown=airflow:root dbt_core_telecoms /opt/airflow/dbt_core_telecoms