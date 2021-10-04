## Base image is Debian Linux with Python 3.7.2
FROM python:3.7.2-slim

## update pip (Python package installer)
RUN pip install --upgrade pip

## copy source code into Docker image
COPY . ./fast-km/

## install package requirements
RUN pip install -r ./fast-km/requirements.txt

## Set the entrypoint of the Docker image to main.py
ENTRYPOINT ["python", "/fast-km/main.py"]