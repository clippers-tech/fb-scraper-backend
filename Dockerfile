FROM python:3.11-slim

# Install system dependencies for Playwright, FFmpeg, and audio processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium
RUN playwright install chromium

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p videos thumbnails exports browser_data

EXPOSE 10000

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "10000"]
