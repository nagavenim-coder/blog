#!/usr/bin/env python3
"""
SEO Data Fetcher

This script fetches SEO data from public sources for movies and stores it
for later use in rewriting synopses.
"""

import os
import json
import time
import random
import requests
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
import re
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("seo_data_fetcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SEODataFetcher")

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"
SEO_DATA_DIR = BASE_DIR / "data/seo"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
]

# Create SEO data directory if it doesn't exist
SEO_DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_random_user_agent() -> str:
    """Return a random user agent from the list."""
    return random.choice(USER_AGENTS)

def fetch_google_trends_data(movie_title: str, movie_year: str, genre: str) -> Dict[str, Any]:
    """
    Fetch related keywords from Google Trends API.
    Note: This is a simulated function as direct access to Google Trends API requires authentication.
    
    Args:
        movie_title: The title of the movie
        movie_year: The release year of the movie
        genre: The genre of the movie
        
    Returns:
        Dictionary with related keywords and their scores
    """
    logger.info(f"Fetching Google Trends data for {movie_title} ({movie_year})")
    
    # In a real implementation, you would use pytrends or a similar library
    # to access Google Trends data. For this example, we'll simulate the response.
    
    # Genre-specific keywords that are commonly searched
    genre_keywords = {
        "Action": ["action movies", "action thriller", "best action films", "action heroes", 
                  "action sequences", "action director", "action star", "fight scenes"],
        "Drama": ["drama films", "emotional movies", "character driven", "dramatic story", 
                 "drama director", "powerful performances", "drama awards", "dramatic tension"],
        "Comedy": ["funny movies", "comedy films", "best comedies", "laugh out loud", 
                  "comedy actors", "humor", "comedic", "hilarious scenes"],
        "Horror": ["scary movies", "horror films", "best horror", "frightening", 
                  "horror director", "terrifying", "horror genre", "horror scenes"],
        "Romance": ["romantic movies", "love story", "romance films", "romantic comedy", 
                   "romantic drama", "love interest", "romantic scenes", "chemistry"],
        "Thriller": ["thriller movies", "suspense films", "mystery thriller", "plot twist", 
                    "suspenseful", "thriller director", "edge of seat", "thriller genre"],
        "SciFi": ["science fiction", "sci-fi movies", "futuristic", "sci-fi director", 
                 "alien", "space", "technology", "sci-fi effects"],
        "Crime": ["crime movies", "detective films", "crime thriller", "crime drama", 
                 "criminal", "investigation", "police", "crime story"]
    }
    
    # Get keywords for the specific genre
    specific_keywords = genre_keywords.get(genre, ["movies", "films", "cinema", "watch online"])
    
    # Movie-specific keywords
    movie_specific = [
        f"{movie_title} movie",
        f"{movie_title} {movie_year}",
        f"{movie_title} {genre.lower()}",
        f"{movie_title} plot",
        f"{movie_title} cast",
        f"{movie_title} review",
        f"{movie_title} watch online",
        f"{movie_title} streaming",
        f"{movie_title} download",
        f"{movie_title} full movie"
    ]
    
    # Combine and assign random scores
    all_keywords = specific_keywords + movie_specific
    keyword_data = {}
    
    for keyword in all_keywords:
        # Assign a random score between 20 and 100
        keyword_data[keyword] = random.randint(20, 100)
    
    # Sort by score (descending)
    sorted_keywords = {k: v for k, v in sorted(keyword_data.items(), key=lambda item: item[1], reverse=True)}
    
    return {
        "source": "google_trends",
        "keywords": sorted_keywords,
        "fetched_date": time.strftime("%Y-%m-%d")
    }

def fetch_related_searches(movie_title: str, movie_year: str) -> Dict[str, Any]:
    """
    Fetch related searches from public sources.
    
    Args:
        movie_title: The title of the movie
        movie_year: The release year of the movie
        
    Returns:
        Dictionary with related searches
    """
    logger.info(f"Fetching related searches for {movie_title} ({movie_year})")
    
    # In a real implementation, you would scrape search engines for related searches.
    # For this example, we'll generate simulated related searches.
    
    related_searches = [
        f"{movie_title} {movie_year} review",
        f"{movie_title} cast",
        f"{movie_title} plot summary",
        f"{movie_title} ending explained",
        f"{movie_title} streaming",
        f"{movie_title} download",
        f"{movie_title} trailer",
        f"{movie_title} director",
        f"{movie_title} box office",
        f"{movie_title} awards",
        f"{movie_title} similar movies",
        f"{movie_title} sequel",
        f"is {movie_title} based on true story",
        f"where to watch {movie_title}",
        f"{movie_title} full movie"
    ]
    
    # Shuffle and select a random number of searches
    random.shuffle(related_searches)
    selected_searches = related_searches[:random.randint(5, len(related_searches))]
    
    return {
        "source": "related_searches",
        "searches": selected_searches,
        "fetched_date": time.strftime("%Y-%m-%d")
    }

def fetch_movie_metadata(movie_title: str, movie_year: str) -> Dict[str, Any]:
    """
    Fetch additional movie metadata from public sources.
    
    Args:
        movie_title: The title of the movie
        movie_year: The release year of the movie
        
    Returns:
        Dictionary with movie metadata
    """
    logger.info(f"Fetching movie metadata for {movie_title} ({movie_year})")
    
    # In a real implementation, you would use APIs like OMDB, TMDB, etc.
    # For this example, we'll generate simulated metadata.
    
    # Common movie metadata tags
    tags = [
        "must-watch", "cult-classic", "award-winning", "critically-acclaimed",
        "box-office-hit", "indie-film", "blockbuster", "low-budget",
        "fan-favorite", "underrated", "overrated", "controversial",
        "family-friendly", "thought-provoking", "visually-stunning",
        "plot-twist", "based-on-book", "based-on-true-story", "sequel",
        "prequel", "remake", "adaptation", "original-screenplay"
    ]
    
    # Select random tags
    selected_tags = random.sample(tags, random.randint(3, 7))
    
    # Generate random ratings
    ratings = {
        "imdb": round(random.uniform(5.0, 9.5), 1),
        "rotten_tomatoes": random.randint(50, 95),
        "metacritic": random.randint(50, 90)
    }
    
    return {
        "source": "movie_metadata",
        "tags": selected_tags,
        "ratings": ratings,
        "fetched_date": time.strftime("%Y-%m-%d")
    }

def fetch_seo_data_for_movie(movie_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fetch SEO data for a movie from various sources.
    
    Args:
        movie_data: Dictionary containing movie information
        
    Returns:
        Dictionary with SEO data
    """
    title = movie_data.get("title", "")
    year = movie_data.get("year", "")
    genre = movie_data.get("genre", "")
    
    logger.info(f"Fetching SEO data for {title} ({year})")
    
    # Fetch data from different sources
    google_trends = fetch_google_trends_data(title, year, genre)
    related_searches = fetch_related_searches(title, year)
    movie_metadata = fetch_movie_metadata(title, year)
    
    # Extract keywords from meta_keywords field if available
    meta_keywords = []
    if "meta_keywords" in movie_data:
        meta_keywords_str = movie_data.get("meta_keywords", "")
        meta_keywords = [kw.strip() for kw in meta_keywords_str.split(",") if kw.strip()]
    
    # Extract keywords from meta_description field if available
    meta_description_keywords = []
    if "meta_description" in movie_data:
        meta_desc = movie_data.get("meta_description", "")
        # Extract potential keywords (words longer than 4 characters)
        meta_description_keywords = [word.lower() for word in re.findall(r'\b\w{5,}\b', meta_desc)]
    
    # Combine all SEO data
    seo_data = {
        "movie_id": movie_data.get("id", ""),
        "movie_title": title,
        "movie_year": year,
        "google_trends": google_trends,
        "related_searches": related_searches,
        "movie_metadata": movie_metadata,
        "meta_keywords": meta_keywords,
        "meta_description_keywords": meta_description_keywords,
        "fetched_date": time.strftime("%Y-%m-%d")
    }
    
    return seo_data

def process_movie_file(movie_file_path: Path) -> None:
    """
    Process a single movie file and fetch SEO data.
    
    Args:
        movie_file_path: Path to the movie JSON file
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
        
        # Check if SEO data already exists
        seo_file_path = SEO_DATA_DIR / f"{movie_id}_seo.json"
        if seo_file_path.exists():
            # Check if the data is recent (less than 30 days old)
            with open(seo_file_path, 'r') as f:
                existing_data = json.load(f)
            
            fetched_date = existing_data.get("fetched_date", "")
            if fetched_date:
                try:
                    # Parse the date
                    fetched_time = time.strptime(fetched_date, "%Y-%m-%d")
                    current_time = time.localtime()
                    
                    # Calculate days difference
                    fetched_days = time.mktime(fetched_time) / (60 * 60 * 24)
                    current_days = time.mktime(current_time) / (60 * 60 * 24)
                    days_diff = current_days - fetched_days
                    
                    if days_diff < 30:
                        logger.info(f"Recent SEO data exists for {movie_title}. Skipping.")
                        return
                    else:
                        logger.info(f"SEO data for {movie_title} is older than 30 days. Refreshing.")
                except ValueError:
                    # If date parsing fails, fetch new data
                    pass
        
        # Fetch SEO data
        seo_data = fetch_seo_data_for_movie(movie_data)
        
        # Save SEO data
        with open(seo_file_path, 'w') as f:
            json.dump(seo_data, f, indent=2)
        
        logger.info(f"Saved SEO data for {movie_title} to {seo_file_path}")
        
    except Exception as e:
        logger.error(f"Error processing {movie_file_path}: {str(e)}")

def extract_top_keywords(seo_data_dir: Path = SEO_DATA_DIR) -> Dict[str, List[str]]:
    """
    Extract top keywords from all SEO data files.
    
    Args:
        seo_data_dir: Directory containing SEO data files
        
    Returns:
        Dictionary with genre-specific keywords
    """
    logger.info("Extracting top keywords from SEO data")
    
    # Initialize genre-specific keyword dictionaries
    genre_keywords = {}
    
    # Process all SEO data files
    for seo_file in seo_data_dir.glob("*_seo.json"):
        try:
            with open(seo_file, 'r') as f:
                seo_data = json.load(f)
            
            # Get movie data
            movie_id = seo_data.get("movie_id", "")
            movie_title = seo_data.get("movie_title", "")
            
            # Find corresponding movie file to get genre
            movie_files = list(MOVIES_DIR.glob(f"*-{movie_id}*.json"))
            if not movie_files:
                continue
                
            with open(movie_files[0], 'r') as f:
                movie_data = json.load(f)
            
            genre = movie_data.get("genre", "Unknown")
            
            # Initialize genre entry if not exists
            if genre not in genre_keywords:
                genre_keywords[genre] = {}
            
            # Extract keywords from Google Trends
            if "google_trends" in seo_data and "keywords" in seo_data["google_trends"]:
                for keyword, score in seo_data["google_trends"]["keywords"].items():
                    if keyword not in genre_keywords[genre]:
                        genre_keywords[genre][keyword] = score
                    else:
                        # Update score if higher
                        genre_keywords[genre][keyword] = max(genre_keywords[genre][keyword], score)
            
            # Extract keywords from meta_keywords
            if "meta_keywords" in seo_data:
                for keyword in seo_data["meta_keywords"]:
                    if keyword not in genre_keywords[genre]:
                        genre_keywords[genre][keyword] = 50  # Default score
                    
        except Exception as e:
            logger.error(f"Error processing SEO file {seo_file}: {str(e)}")
    
    # Sort keywords by score and take top 20 for each genre
    top_keywords = {}
    for genre, keywords in genre_keywords.items():
        sorted_keywords = sorted(keywords.items(), key=lambda x: x[1], reverse=True)
        top_keywords[genre] = [k for k, v in sorted_keywords[:20]]
    
    # Save top keywords to a file
    top_keywords_file = SEO_DATA_DIR / "top_keywords.json"
    with open(top_keywords_file, 'w') as f:
        json.dump(top_keywords, f, indent=2)
    
    logger.info(f"Saved top keywords to {top_keywords_file}")
    
    return top_keywords

def main() -> None:
    """Main function to process all movie files."""
    logger.info("Starting SEO data fetching process")
    
    # Check if movies directory exists
    if not MOVIES_DIR.exists() or not MOVIES_DIR.is_dir():
        logger.error(f"Movies directory {MOVIES_DIR} does not exist")
        return
    
    # Get all movie files
    movie_files = list(MOVIES_DIR.glob("*.json"))
    logger.info(f"Found {len(movie_files)} movie files")
    
    # Process each movie file
    for i, movie_file in enumerate(movie_files):
        logger.info(f"Processing movie {i+1}/{len(movie_files)}: {movie_file.name}")
        process_movie_file(movie_file)
        
        # Add a small delay between processing files
        time.sleep(random.uniform(0.5, 1.5))
    
    # Extract top keywords from all SEO data
    top_keywords = extract_top_keywords()
    
    logger.info("SEO data fetching process completed")

if __name__ == "__main__":
    main()
