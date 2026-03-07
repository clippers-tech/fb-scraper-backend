#!/bin/bash
# Render build script — installs Python deps + Playwright + Chromium

set -e

echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Installing Playwright system deps + Chromium..."
playwright install --with-deps chromium

echo "Creating data directories..."
mkdir -p videos thumbnails exports browser_data

echo "Build complete."
