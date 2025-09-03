#!/usr/bin/env python3
"""
Generate SEO Hashtags Script

Generates hashtags for movies based on plot, SEO keywords, and movie data to drive traffic to ShemarooMe.
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
SEO_DATA_DIR = BASE_DIR / "data/seo"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-5-haiku-20241022-v1:0"

def get_seo_data(movie_id):
    """Get SEO data for a movie."""
    seo_file_path = SEO_DATA_DIR / f"{movie_id}_seo.json"
    if seo_file_path.exists():
        try:
            with open(seo_file_path, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def generate_hashtags(movie_data, seo_data):
    """Generate hashtags using Bedrock."""
    try:
        bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        title = movie_data.get('title', '')
        year = movie_data.get('year', '')
        genre = movie_data.get('genre', '')
        plot = movie_data.get('web_plot', movie_data.get('plot', ''))
        cast = movie_data.get('cast', [])[:3]
        director = movie_data.get('director', '')
        
        # Extract SEO keywords
        seo_keywords = []
        if 'google_trends' in seo_data and 'keywords' in seo_data['google_trends']:
            seo_keywords = list(seo_data['google_trends']['keywords'].keys())[:10]
        
        meta_keywords = movie_data.get('meta_keywords', '')
        
        prompt = f"""Generate 15-20 SEO hashtags for the movie "{title} ({year})" to drive traffic to ShemarooMe streaming platform.

Movie Details:
- Genre: {genre}
- Director: {director}
- Cast: {', '.join(cast)}
- Plot: {plot[:500]}...

SEO Keywords: {', '.join(seo_keywords)}
Meta Keywords: {meta_keywords}

Create hashtags that include:
- Movie title and variations
- Genre and movie type hashtags
- Cast member names
- Director name
- Streaming and platform hashtags
- Popular movie search terms
- ShemarooMe branding

Format: Return only hashtags separated by spaces, starting each with #
Example: #MovieTitle #BollywoodMovies #StreamOnline #ShemarooMe

Generate hashtags:"""

        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 300,
            "temperature": 0.5,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_payload)
        )
        
        response_body = json.loads(response.get('body').read())
        hashtags_text = response_body.get('content', [{}])[0].get('text', '').strip()
        
        # Clean and format hashtags
        hashtags = []
        for tag in hashtags_text.split():
            if tag.startswith('#') and len(tag) > 1:
                hashtags.append(tag)
        
        return hashtags[:20] if hashtags else None
        
    except Exception as e:
        logging.error(f"Bedrock error for {title}: {e}")
        return None

def add_hashtags_to_movie(movie_file_path):
    """Add hashtags to a movie JSON file."""
    try:
        with open(movie_file_path, 'r', encoding='utf-8') as f:
            movie_data = json.load(f)
        
        title = movie_data.get('title', '')
        movie_id = movie_data.get('id', '')
        
        if 'seo_hashtags' in movie_data:
            logging.info(f"Skipping {title} - already has hashtags")
            return
        
        logging.info(f"Generating hashtags for {title}")
        
        # Get SEO data
        seo_data = get_seo_data(movie_id)
        
        # Generate hashtags
        hashtags = generate_hashtags(movie_data, seo_data)
        
        if hashtags:
            movie_data['seo_hashtags'] = hashtags
            movie_data['last_updated'] = '2025-07-01'
            
            with open(movie_file_path, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Added {len(hashtags)} hashtags for {title}")
        else:
            logging.warning(f"Could not generate hashtags for {title}")
            
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
        add_hashtags_to_movie(movie_file)
    
    logging.info("Hashtag generation completed")

if __name__ == "__main__":
    main()