FROM bitnami/spark:3.4.1

USER root

RUN mkdir -p /home/spark/working_dir

USER 1001

RUN python -m pip install --upgrade pip
RUN python -m pip install nba_api

# docker build -t my_spark