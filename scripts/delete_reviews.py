#!/usr/bin/env python3
"""
Delete all review files to allow regeneration
"""

import os
import shutil
from pathlib import Path

# Path to reviews directory
BASE_DIR = Path(__file__).parent.parent
REVIEWS_DIR = BASE_DIR / "data/reviews"

# Check if directory exists
if REVIEWS_DIR.exists() and REVIEWS_DIR.is_dir():
    # Delete all files in the directory
    for file in REVIEWS_DIR.glob("*.json"):
        print(f"Deleting {file}")
        file.unlink()
    
    print("All review files deleted successfully.")
else:
    print(f"Reviews directory {REVIEWS_DIR} does not exist.")
