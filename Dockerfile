## Base image is Debian Linux with Python 3.7.2
FROM python:3.7.2-slim

## copy source code into Docker image
COPY . ./fast-km/

WORKDIR /fast-km/

## install python package requirements
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

## start supervisor daemon to manage worker processes
#RUN ./usr/local/bin/supervisord -c ./fast-km/supervisord/supervisord.conf

## expose port 5000 for web access to container
EXPOSE 5000

RUN chmod +x healthcheck.sh
HEALTHCHECK --interval=30s --timeout=3s CMD ./healthcheck.sh

## Set the entrypoint of the Docker image to app.py
ENTRYPOINT ["python", "-u", "app.py"]