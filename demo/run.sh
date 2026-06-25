#!/bin/sh
# Pokretanje demo aplikacije (Linux/macOS)
# Usage: ./run.sh [config_path]
# Ako se ne navede putanja, koristi se ./demo/config.yaml

CONFIG_PATH="${1:-./demo/config.yaml}"

if [ ! -f "$CONFIG_PATH" ]; then
    echo "Config file not found: $CONFIG_PATH"
    exit 1
fi

echo "Using config: $CONFIG_PATH"
export DEMO_CONFIG="$CONFIG_PATH"
go run ./cmd/demo/
