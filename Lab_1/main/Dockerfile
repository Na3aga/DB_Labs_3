FROM python:3.9.1-slim-buster

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && pip install --upgrade pip && apt install --no-install-recommends gcc -y
COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

COPY . /app/
