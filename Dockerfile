FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

CMD gunicorn -w 1 --worker-class gthread --threads 4 \
    -b 0.0.0.0:${PORT} \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    app:app
