#!/usr/bin/env python3
"""
Clean Movie Data Script

Removes web_plot, raw_web_plot, and plot_source fields from all movie JSON files.
"""

import json
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"

def clean_movie_file(movie_file_path):
    """Remove unwanted fields from a movie JSON file."""
    try:
        with open(movie_file_path, 'r', encoding='utf-8') as f:
            movie_data = json.load(f)
        
        # Remove unwanted fields
        fields_to_remove = ['web_plot', 'raw_web_plot', 'plot_source']
        removed_fields = []
        
        for field in fields_to_remove:
            if field in movie_data:
                del movie_data[field]
                removed_fields.append(field)
        
        if removed_fields:
            with open(movie_file_path, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Cleaned {movie_file_path.name}: removed {removed_fields}")
        else:
            logging.info(f"No cleanup needed for {movie_file_path.name}")
            
    except Exception as e:
        logging.error(f"Error processing {movie_file_path}: {e}")

def main():
    """Process all movie files."""
    if not MOVIES_DIR.exists():
        logging.error(f"Movies directory not found: {MOVIES_DIR}")
        return
    
    movie_files = list(MOVIES_DIR.glob("*.json"))
    logging.info(f"Found {len(movie_files)} movie files")
    
    for movie_file in movie_files:
        clean_movie_file(movie_file)
    
    logging.info("Cleanup completed")

if __name__ == "__main__":
    main()