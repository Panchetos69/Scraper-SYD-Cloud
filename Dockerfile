FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    ffmpeg \
    rtmpdump \
    && rm -rf /var/lib/apt/lists/*

# Instalar yt-dlp
RUN pip install yt-dlp

# Instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY scraper.py .

# Cloud Run llama a este endpoint HTTP para disparar el scraper
COPY main.py .

CMD ["python", "main.py"]