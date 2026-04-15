FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    rtmpdump \
    && rm -rf /var/lib/apt/lists/*

RUN pip install yt-dlp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py .
COPY main.py .

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "7200", "--workers", "1", "main:app"]
