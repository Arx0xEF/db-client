#!/usr/bin/env bash

set -e

if dpkg -s libpq-dev >/dev/null 2>&1; then
    echo "libpq-dev is already installed."
else
    echo "Installing libpq-dev..."
    sudo apt update
    sudo apt install -y libpq-dev
fi

echo "Setup complete."
