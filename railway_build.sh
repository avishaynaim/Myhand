#!/bin/bash
set -e

echo "=== Pre-build verification ==="
echo "Current directory: $(pwd)"
echo "Files in current directory:"
ls -la

echo ""
echo "=== Checking for templates directory ==="
if [ -d "templates" ]; then
    echo "✓ templates directory exists"
    ls -la templates/
else
    echo "✗ templates directory NOT found"
fi

echo ""
echo "=== Checking for static directory ==="
if [ -d "static" ]; then
    echo "✓ static directory exists"
    find static -type f
else
    echo "✗ static directory NOT found"
fi

echo ""
echo "=== Installing dependencies ==="
pip install -r requirements.txt

echo ""
echo "=== Build complete ==="
