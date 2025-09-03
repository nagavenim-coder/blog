#!/usr/bin/env python3
"""
Add Why Watch Section Script

Adds a "why_watch" section to movie JSON files explaining why viewers should watch the movie.
"""

import json
from pathlib import Path
import logging
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-5-haiku-20241022-v1:0"

def generate_why_watch(movie_data):
    """Generate why watch content using Bedrock."""
    try:
        bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        title = movie_data.get('title', '')
        year = movie_data.get('year', '')
        genre = movie_data.get('genre', '')
        plot = movie_data.get('web_plot', movie_data.get('plot', ''))
        cast = ', '.join(movie_data.get('cast', [])[:3])
        director = movie_data.get('director', '')
        
        prompt = f"""Write a compelling "Why You Should Watch" section for the movie "{title} ({year})" for a movie blog.

Movie Details:
- Genre: {genre}
- Director: {director}
- Cast: {cast}
- Plot: {plot}

Create 3-4 engaging paragraphs (150-200 words total) that:
- Highlight the movie's strengths and appeal
- Mention what makes it worth watching
- Focus on entertainment value, performances, or unique elements
- Use an enthusiastic but professional tone
- DO NOT include spoilers
- DO NOT ask readers to write reviews
- DO NOT use phrases like "Don't miss" or "Must watch"

Write only the content, no headings or titles."""

        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 500,
            "temperature": 0.7,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_payload)
        )
        
        response_body = json.loads(response.get('body').read())
        why_watch = response_body.get('content', [{}])[0].get('text', '').strip()
        
        return why_watch if len(why_watch) >= 100 else None
        
    except Exception as e:
        logging.error(f"Bedrock error for {title}: {e}")
        return None

def add_why_watch_section(movie_file_path):
    """Add why_watch section to a movie JSON file."""
    try:
        with open(movie_file_path, 'r', encoding='utf-8') as f:
            movie_data = json.load(f)
        
        title = movie_data.get('title', '')
        
        if 'why_watch' in movie_data:
            logging.info(f"Skipping {title} - already has why_watch section")
            return
        
        logging.info(f"Generating why_watch for {title}")
        
        why_watch = generate_why_watch(movie_data)
        
        if why_watch:
            movie_data['why_watch'] = why_watch
            movie_data['last_updated'] = '2025-07-01'
            
            with open(movie_file_path, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Added why_watch for {title}")
        else:
            logging.warning(f"Could not generate why_watch for {title}")
            
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
        add_why_watch_section(movie_file)
    
    logging.info("Why watch generation completed")

if __name__ == "__main__":
    main()