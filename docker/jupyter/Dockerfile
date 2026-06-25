FROM jupyter/pyspark-notebook:latest

USER root

COPY requirements.txt .

RUN pip install -r requirements.txt

USER jovyan