FROM python:3-alpine3.10
LABEL authors="mirjalal"

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app", "&", "celery", "-A", "apps.core.celery_app", "worker", "--loglevel=info"]
