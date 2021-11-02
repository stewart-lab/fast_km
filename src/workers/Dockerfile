## Base image is Debian Linux with Python 3.7.2
FROM python:3.8.9-slim

## copy source code into Docker image
COPY ./.. /fast-km/

WORKDIR /fast-km/

## install python package requirements
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

## Default command on running the container
ENTRYPOINT ["python", "-u", "src/run_worker.py"]