FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD gunicorn --worker-class eventlet -w 1 -b 0.0.0.0:$PORT --timeout 120 --access-logfile - --error-logfile - app:app
