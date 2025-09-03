#!/usr/bin/env python3
"""
Movie Synopsis Rewriter with SEO Data

This script rewrites movie synopses using SEO data collected from public sources
and stores them as a separate field in the movie JSON files.
"""

import os
import json
import time
import random
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
import boto3
import botocore.exceptions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("seo_synopsis_rewriter.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SEOSynopsisRewriter")

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"
SEO_DATA_DIR = BASE_DIR / "data/seo"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-5-haiku-20241022-v1:0"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

def create_bedrock_client():
    """Create and return a Bedrock client."""
    try:
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name="us-east-1"  # Change to your preferred region
        )
        return bedrock_runtime
    except Exception as e:
        logger.error(f"Error creating Bedrock client: {str(e)}")
        return None

def get_seo_data(movie_id: str) -> Dict[str, Any]:
    """
    Get SEO data for a movie.
    
    Args:
        movie_id: The ID of the movie
        
    Returns:
        Dictionary with SEO data or empty dict if not found
    """
    seo_file_path = SEO_DATA_DIR / f"{movie_id}_seo.json"
    if not seo_file_path.exists():
        logger.warning(f"No SEO data found for movie ID: {movie_id}")
        return {}
    
    try:
        with open(seo_file_path, 'r') as f:
            seo_data = json.load(f)
        return seo_data
    except Exception as e:
        logger.error(f"Error reading SEO data for {movie_id}: {str(e)}")
        return {}

def extract_top_keywords(seo_data: Dict[str, Any], max_keywords: int = 10) -> List[str]:
    """
    Extract top keywords from SEO data.
    
    Args:
        seo_data: Dictionary containing SEO data
        max_keywords: Maximum number of keywords to extract
        
    Returns:
        List of top keywords
    """
    keywords = []
    
    # Extract from Google Trends
    if "google_trends" in seo_data and "keywords" in seo_data["google_trends"]:
        trends_keywords = list(seo_data["google_trends"]["keywords"].keys())
        keywords.extend(trends_keywords[:max_keywords])
    
    # Extract from meta_keywords
    if "meta_keywords" in seo_data:
        meta_keywords = seo_data.get("meta_keywords", [])
        keywords.extend(meta_keywords[:max_keywords])
    
    # Extract from related searches
    if "related_searches" in seo_data and "searches" in seo_data["related_searches"]:
        related_searches = seo_data["related_searches"]["searches"]
        # Extract individual words from related searches
        for search in related_searches[:5]:  # Limit to first 5 searches
            words = search.split()
            keywords.extend([word for word in words if len(word) > 3])
    
    # Remove duplicates and limit to max_keywords
    unique_keywords = []
    for keyword in keywords:
        if keyword.lower() not in [k.lower() for k in unique_keywords]:
            unique_keywords.append(keyword)
    
    return unique_keywords[:max_keywords]

def generate_synopsis_with_bedrock(client, movie_data: Dict[str, Any], seo_data: Dict[str, Any]) -> str:
    """
    Generate a movie synopsis using AWS Bedrock's Claude Haiku model and SEO data.
    
    Args:
        client: Bedrock client
        movie_data: Dictionary containing movie information
        seo_data: Dictionary containing SEO data
        
    Returns:
        AI-generated synopsis
    """
    # Extract movie information
    title = movie_data.get("title", "")
    year = movie_data.get("year", "")
    genre = movie_data.get("genre", "")
    plot = movie_data.get("web_plot", movie_data.get("plot", ""))
    director = movie_data.get("director", "")
    cast = movie_data.get("cast", [])
    
    # Extract SEO keywords
    seo_keywords = extract_top_keywords(seo_data)
    keywords_str = ", ".join(seo_keywords)
    
    # Extract movie tags if available
    tags = []
    if "movie_metadata" in seo_data and "tags" in seo_data["movie_metadata"]:
        tags = seo_data["movie_metadata"]["tags"]
    tags_str = ", ".join(tags)
    
    # Extract ratings if available
    ratings = {}
    if "movie_metadata" in seo_data and "ratings" in seo_data["movie_metadata"]:
        ratings = seo_data["movie_metadata"]["ratings"]
    ratings_info = ""
    if ratings:
        ratings_info = f"IMDb: {ratings.get('imdb', 'N/A')}, Rotten Tomatoes: {ratings.get('rotten_tomatoes', 'N/A')}%, Metacritic: {ratings.get('metacritic', 'N/A')}"
    
    # Create a prompt for Claude Haiku
    prompt = f"""
    You are an expert movie synopsis writer specializing in SEO-optimized content.
    
    Rewrite the following movie synopsis to make it engaging, descriptive, and SEO-friendly.
    
    Movie: {title} ({year})
    Genre: {genre}
    Director: {director}
    Cast: {', '.join(cast[:3]) if cast else 'Unknown'}
    
    Original Synopsis: {plot}
    
    SEO Keywords to incorporate naturally: {keywords_str}
    
    Movie Tags: {tags_str}
    
    Ratings: {ratings_info}
    
    Create an SEO-optimized synopsis that:
    1. Is approximately 2-3 sentences long
    2. Starts with an engaging hook that mentions the title, year, and genre
    3. Naturally incorporates the SEO keywords
    4. Preserves the key plot points
    5. Ends with a question or teaser if appropriate
    6. Has a total length of 50-100 words
    
    Write ONLY the new synopsis, without any explanations or additional text.
    """
    
    # Prepare the request payload for Claude Haiku
    request_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 300,
        "temperature": 0.7,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }
    
    # Invoke the model with retries
    for attempt in range(MAX_RETRIES):
        try:
            response = client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_payload)
            )
            
            # Parse the response
            response_body = json.loads(response.get('body').read())
            generated_text = response_body.get('content', [{}])[0].get('text', '')
            
            # Clean up the response if needed
            generated_text = generated_text.strip()
            
            logger.info(f"Successfully generated synopsis for {title}")
            return generated_text
            
        except botocore.exceptions.ClientError as error:
            logger.error(f"AWS Bedrock error on attempt {attempt+1}: {str(error)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
            else:
                logger.error(f"Failed to generate synopsis after {MAX_RETRIES} attempts")
                return fallback_synopsis_generation(movie_data, seo_data)
                
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt+1}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (2 ** attempt))
            else:
                logger.error(f"Failed to generate synopsis after {MAX_RETRIES} attempts")
                return fallback_synopsis_generation(movie_data, seo_data)
    
    # If we get here, all attempts failed
    return fallback_synopsis_generation(movie_data, seo_data)

def fallback_synopsis_generation(movie_data: Dict[str, Any], seo_data: Dict[str, Any]) -> str:
    """
    Generate a fallback synopsis when Bedrock fails.
    
    Args:
        movie_data: Dictionary containing movie information
        seo_data: Dictionary containing SEO data
        
    Returns:
        Generated synopsis
    """
    # Extract movie information
    title = movie_data.get("title", "")
    year = movie_data.get("year", "")
    genre = movie_data.get("genre", "")
    plot = movie_data.get("web_plot", movie_data.get("plot", ""))
    director = movie_data.get("director", "")
    
    # Extract SEO keywords
    seo_keywords = extract_top_keywords(seo_data, 5)  # Limit to 5 keywords for fallback
    
    # Create a simple template-based synopsis
    if not seo_keywords:
        seo_keywords = ["must-watch", "entertaining", "classic", "popular", "acclaimed"]
    
    # Shorten the plot if needed
    if len(plot) > 150:
        shortened_plot = plot[:150] + "..."
    else:
        shortened_plot = plot
    
    # Create fallback synopsis
    fallback = f"{title} ({year}) is a {seo_keywords[0]} {genre.lower()} film directed by {director}. {shortened_plot} This {seo_keywords[1]} story will keep audiences engaged from start to finish."
    
    logger.warning(f"Using fallback synopsis generation for {title}")
    return fallback

def process_movie_file(movie_file_path: Path, bedrock_client) -> None:
    """
    Process a single movie file and rewrite its synopsis using SEO data.
    
    Args:
        movie_file_path: Path to the movie JSON file
        bedrock_client: AWS Bedrock client
    """
    try:
        # Read movie data
        with open(movie_file_path, 'r') as f:
            movie_data = json.load(f)
        
        movie_id = movie_data.get("id", "")
        movie_title = movie_data.get("title", "")
        
        if not movie_id:
            logger.warning(f"Missing ID in {movie_file_path}")
            return
        
        # Get SEO data
        seo_data = get_seo_data(movie_id)
        if not seo_data:
            logger.warning(f"No SEO data available for {movie_title}. Using basic rewrite.")
        
        # Generate AI synopsis using Bedrock and SEO data
        ai_synopsis = generate_synopsis_with_bedrock(bedrock_client, movie_data, seo_data)
        
        # Add AI synopsis to movie data
        movie_data["seo_synopsis"] = ai_synopsis
        
        # Update last_updated field
        movie_data["last_updated"] = time.strftime("%Y-%m-%d")
        
        # Write updated movie data back to file
        with open(movie_file_path, 'w') as f:
            json.dump(movie_data, f, indent=2)
        
        logger.info(f"Added SEO-optimized synopsis for {movie_title}")
        
    except Exception as e:
        logger.error(f"Error processing {movie_file_path}: {str(e)}")

def main() -> None:
    """Main function to process all movie files."""
    logger.info("Starting SEO synopsis generation process")
    
    # Create Bedrock client
    bedrock_client = create_bedrock_client()
    if not bedrock_client:
        logger.error("Failed to create Bedrock client. Exiting.")
        return
    
    # Check if movies directory exists
    if not MOVIES_DIR.exists() or not MOVIES_DIR.is_dir():
        logger.error(f"Movies directory {MOVIES_DIR} does not exist")
        return
    
    # Check if SEO data directory exists
    if not SEO_DATA_DIR.exists() or not SEO_DATA_DIR.is_dir():
        logger.error(f"SEO data directory {SEO_DATA_DIR} does not exist")
        return
    
    # Get all movie files
    movie_files = list(MOVIES_DIR.glob("*.json"))
    logger.info(f"Found {len(movie_files)} movie files")
    
    # Process each movie file
    for i, movie_file in enumerate(movie_files):
        logger.info(f"Processing movie {i+1}/{len(movie_files)}: {movie_file.name}")
        process_movie_file(movie_file, bedrock_client)
        
        # Add a delay between API calls to avoid rate limiting
        time.sleep(random.uniform(1.0, 2.0))
    
    logger.info("SEO synopsis generation process completed")

if __name__ == "__main__":
    main()
