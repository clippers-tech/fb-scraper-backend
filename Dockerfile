FROM python:3.11-slim

# Install FFmpeg for video processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    libgl1-mesa-glx \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p videos thumbnails exports

EXPOSE 10000

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "10000"]
