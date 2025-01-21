#!/bin/bash

# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -e .

# Create necessary directories if they don't exist
mkdir -p src/examples

# Set up pre-commit hooks
if [ -f ".git/hooks/pre-commit" ]; then
    echo "Installing pre-commit hooks..."
    pre-commit install
fi

echo "Setup complete! Don't forget to:"
echo "1. Copy .env.example to .env and fill in your values"
echo "2. Run 'source venv/bin/activate' to activate the virtual environment"
echo "3. Run tests with 'pytest' to verify installation"