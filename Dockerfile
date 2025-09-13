## base image with python 3.12
FROM python:3.12

## copy requirements file into Docker image
COPY requirements.txt /app/requirements.txt

## set working directory
WORKDIR /app

## install python package requirements
RUN pip install --upgrade pip
RUN pip install -q -r requirements.txt
RUN pip install htcondor

## copy source code into Docker image.
## this is done last to take advantage of Docker layer caching
## so that we don't have to reinstall packages if only source code changes.
COPY src /app/src
COPY app.py /app/app.py

## run the app on running the container
ENTRYPOINT ["python", "-u", "app.py"]