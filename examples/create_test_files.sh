#!/bin/bash

# Create benchmark test files using dd
# This script creates test files of various sizes for S3 performance benchmarking

set -e  # Exit on any error

echo "Creating test_data directory..."
mkdir -p test_data

echo "Creating test files..."

echo "  Creating 5MB file..."
dd if=/dev/urandom of=test_data/5mb.bin bs=1M count=5 2>/dev/null

echo "  Creating 100MB file..."
dd if=/dev/urandom of=test_data/100mb.bin bs=1M count=100 2>/dev/null

echo "  Creating 250MB file..."
dd if=/dev/urandom of=test_data/250mb.bin bs=1M count=250 2>/dev/null

echo "  Creating 500MB file..."
dd if=/dev/urandom of=test_data/500mb.bin bs=1M count=500 2>/dev/null

echo "  Creating 2.5GB file..."
dd if=/dev/urandom of=test_data/2.5gb.bin bs=1M count=2560 2>/dev/null

echo "  Creating 5GB file..."
dd if=/dev/urandom of=test_data/5gb.bin bs=1M count=5120 2>/dev/null

echo "Test files created successfully:"
ls -lh test_data/

echo "Total disk space used:"
du -sh test_data/
