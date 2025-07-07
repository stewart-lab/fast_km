## Base image is Debian Linux with Python 3.8.9
FROM python:3.8.9-slim

## copy source code into Docker image (ignoring .env which could contain sensitive information)
COPY src /fast-km/src
COPY requirements.txt /fast-km/requirements.txt

WORKDIR /fast-km

## install python package requirements
RUN pip install --upgrade pip
RUN pip install -q -r requirements.txt

## Default command on running the container
ENTRYPOINT ["python", "-u", "src/run_worker.py"]
