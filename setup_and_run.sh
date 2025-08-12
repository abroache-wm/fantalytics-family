#!/bin/bash

# ESPN Fantasy Data Fetcher Setup Script
# This script sets up a Python virtual environment and runs the data fetcher

echo "========================================="
echo "ESPN Fantasy Data Fetcher Setup"
echo "========================================="

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip --quiet

# Install required packages
echo "Installing required packages..."
pip install requests pandas --quiet
echo "✓ Packages installed"

# Check if the fetcher script exists
FETCHER_SCRIPT="espn_fantasy_fetcher.py"
if [ ! -f "$FETCHER_SCRIPT" ]; then
    echo "Error: $FETCHER_SCRIPT not found!"
    echo "Please make sure the Python fetcher script is in the current directory."
    exit 1
fi

# Run the fetcher
echo ""
echo "========================================="
echo "Running ESPN Fantasy Data Fetcher"
echo "========================================="
echo ""

python "$FETCHER_SCRIPT"

# Check if the script ran successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "✓ Data fetching completed successfully!"
    echo "========================================="
    echo ""
    echo "Generated files:"
    [ -f "espn_fantasy_matchups.csv" ] && echo "  • espn_fantasy_matchups.csv"
    [ -f "espn_fantasy_standings.csv" ] && echo "  • espn_fantasy_standings.csv"
    [ -f "espn_fantasy_draft_picks.csv" ] && echo "  • espn_fantasy_draft_picks.csv"
    [ -f "espn_fantasy_complete_data.json" ] && echo "  • espn_fantasy_complete_data.json"
    [ -f "espn_fantasy_draft_data.json" ] && echo "  • espn_fantasy_draft_data.json"
else
    echo ""
    echo "❌ Error: Data fetching failed"
    echo "Please check the error messages above"
fi

# Deactivate virtual environment
deactivate

echo ""
echo "Virtual environment deactivated."
echo "To manually activate it again, run: source .venv/bin/activate"