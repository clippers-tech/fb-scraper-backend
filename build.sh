#!/bin/bash
# Render build script — installs Python deps + Playwright + Chromium

set -e

echo "=== Installing Python dependencies ==="
pip install --upgrade pip

# Install CPU-only PyTorch first to avoid pulling GPU version
pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu

# Install remaining dependencies
pip install -r requirements.txt

echo "=== Installing Playwright system deps + Chromium ==="
playwright install --with-deps chromium

echo "=== Creating data directories ==="
mkdir -p videos thumbnails exports browser_data

echo "=== Build complete ==="
