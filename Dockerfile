FROM python:3.12-slim

RUN apt-get update && apt-get install libsecret-1-dev make -y
RUN mkdir /app
WORKDIR /app
COPY . /app

RUN pip install -r /app/requirements.txt --no-cache
