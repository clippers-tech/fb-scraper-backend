#!/bin/bash
# Render build script — installs Python dependencies (no Playwright needed)

set -e

echo "=== Installing Python dependencies ==="
pip install --upgrade pip
pip install -r requirements.txt

echo "=== Creating data directories ==="
mkdir -p videos thumbnails exports

echo "=== Build complete ==="
