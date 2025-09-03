#!/usr/bin/env python3
"""
Movie Blog Generator with Tailwind CSS

This script generates HTML blog pages for each movie using Tailwind CSS for styling,
incorporating data from the movie files and their reviews.
"""

import os
import json
import time
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
import re
import html

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("blog_generator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MovieBlogGenerator")

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"
REVIEWS_DIR = BASE_DIR / "data/reviews"
BLOGS_DIR = BASE_DIR / "blogs"

# Create blogs directory if it doesn't exist
BLOGS_DIR.mkdir(parents=True, exist_ok=True)

def slugify(text: str) -> str:
    """
    Convert text to a URL-friendly slug.
    
    Args:
        text: Text to convert
        
    Returns:
        URL-friendly slug
    """
    # Convert to lowercase
    text = text.lower()
    # Replace spaces with hyphens
    text = re.sub(r'\s+', '-', text)
    # Remove special characters
    text = re.sub(r'[^\w\-]', '', text)
    # Remove duplicate hyphens
    text = re.sub(r'-+', '-', text)
    # Remove leading/trailing hyphens
    text = text.strip('-')
    
    return text

def get_movie_data(movie_file_path: Path) -> Dict[str, Any]:
    """
    Get movie data from a movie file.
    
    Args:
        movie_file_path: Path to the movie JSON file
        
    Returns:
        Dictionary with movie data
    """
    try:
        with open(movie_file_path, 'r') as f:
            movie_data = json.load(f)
        return movie_data
    except Exception as e:
        logger.error(f"Error reading movie data from {movie_file_path}: {str(e)}")
        return {}

def get_movie_reviews(movie_id: str) -> List[Dict[str, Any]]:
    """
    Get reviews for a movie.
    
    Args:
        movie_id: ID of the movie
        
    Returns:
        List of review dictionaries
    """
    review_file_path = REVIEWS_DIR / f"{movie_id}_reviews.json"
    if not review_file_path.exists():
        logger.warning(f"No reviews found for movie ID: {movie_id}")
        return []
    
    try:
        with open(review_file_path, 'r') as f:
            review_data = json.load(f)
        return review_data.get("reviews", [])
    except Exception as e:
        logger.error(f"Error reading reviews for {movie_id}: {str(e)}")
        return []

def generate_movie_blog(movie_data: Dict[str, Any], reviews: List[Dict[str, Any]]) -> str:
    """
    Generate HTML blog page for a movie using Tailwind CSS.
    
    Args:
        movie_data: Dictionary with movie data
        reviews: List of review dictionaries
        
    Returns:
        HTML content for the blog page
    """
    try:
        # Extract movie data
        title = movie_data.get("title", "Unknown Title")
        year = movie_data.get("year", "Unknown Year")
        genre = movie_data.get("genre", "Unknown Genre")
        duration = movie_data.get("duration", "Unknown Duration")
        language = movie_data.get("language", "Unknown Language")
        content_rating = movie_data.get("content_rating", "Not Rated")
        streaming_quality = movie_data.get("streaming_quality", "Unknown Quality")
        seo_synopsis = movie_data.get("seo_synopsis", "No synopsis available.")
        director = movie_data.get("director", "Unknown Director")
        cast = movie_data.get("cast", [])
        poster_url = movie_data.get("poster_url", "")
        watch_url = movie_data.get("watch_url", "#")
        meta_description = movie_data.get("meta_description", f"Watch {title} ({year}) online. Stream {title} movie on ShemarooMe.")
        meta_keywords = movie_data.get("meta_keywords", f"{title}, {title} movie, watch {title} online")
        content_advisory = movie_data.get("content_advisory", "")
        last_updated = movie_data.get("last_updated", time.strftime("%Y-%m-%d"))
        
        # Generate cast list HTML
        cast_html = ""
        for actor in cast:
            cast_html += f"""
            <div class="bg-white rounded-lg shadow p-4 transition-transform duration-300 hover:scale-105">
                <p class="font-medium text-gray-800">{html.escape(actor)}</p>
            </div>
            """
        
        # Generate reviews HTML
        reviews_html = ""
        if reviews:
            for review in reviews:
                author = review.get("author", "Anonymous")
                rating = review.get("rating", "N/A")
                content = review.get("content", "No content")
                date = review.get("date", "Unknown date")
                
                # Determine rating color based on rating value
                rating_color = "bg-yellow-500"  # Default
                if isinstance(rating, (int, float)):
                    if rating >= 4:
                        rating_color = "bg-green-500"
                    elif rating >= 2.5:
                        rating_color = "bg-yellow-500"
                    else:
                        rating_color = "bg-red-500"
                
                reviews_html += f"""
                <div class="bg-white rounded-lg shadow p-6 mb-6">
                    <div class="flex justify-between items-center mb-4">
                        <h3 class="text-lg font-bold text-gray-800">{html.escape(author)}</h3>
                        <span class="{rating_color} text-white px-3 py-1 rounded-full font-bold">{rating}/5</span>
                    </div>
                    <p class="text-gray-600 italic mb-4">"{html.escape(content)}"</p>
                    <p class="text-right text-sm text-gray-500">{html.escape(date)}</p>
                </div>
                """
        else:
            reviews_html = '<p class="text-gray-600 italic">No reviews available for this movie yet.</p>'
        
        # Generate content advisory tags
        content_advisory_tags = ""
        if content_advisory:
            advisories = content_advisory.split(",")
            for advisory in advisories:
                if advisory.strip():
                    content_advisory_tags += f'<span class="bg-red-100 text-red-800 text-xs font-medium px-2.5 py-0.5 rounded-full mr-2">{advisory.strip()}</span>'
        
        # Create HTML content with Tailwind CSS
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html.escape(title)} ({html.escape(str(year))}) - Movie Blog</title>
    <meta name="description" content="{html.escape(meta_description)}">
    <meta name="keywords" content="{html.escape(meta_keywords)}">
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Inter', sans-serif;
        }}
    </style>
</head>
<body class="bg-gray-100 text-gray-900">
    <header class="bg-gradient-to-r from-blue-600 to-indigo-700 text-white py-12 px-4 shadow-lg">
        <div class="container mx-auto max-w-6xl">
            <h1 class="text-4xl md:text-5xl font-bold text-center">{html.escape(title)}</h1>
            <p class="text-xl md:text-2xl text-center mt-2 text-blue-100">{html.escape(str(year))}</p>
        </div>
    </header>
    
    <main class="container mx-auto max-w-6xl px-4 py-8">
        <div class="flex flex-col md:flex-row gap-8 mb-12">
            <!-- Movie Poster -->
            <div class="md:w-1/3 flex justify-center">
                {f'<img src="{poster_url}" alt="{html.escape(title)} Poster" class="rounded-lg shadow-lg max-h-[500px]">' if poster_url else '<div class="bg-gray-200 rounded-lg shadow-lg w-full max-w-[300px] h-[450px] flex items-center justify-center"><p class="text-gray-500 text-center">No Poster Available</p></div>'}
            </div>
            
            <!-- Movie Details -->
            <div class="md:w-2/3">
                <div class="flex flex-wrap gap-2 mb-6">
                    <span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(genre)}</span>
                    <span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(duration)}</span>
                    <span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(language)}</span>
                    <span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(content_rating)}</span>
                    <span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(streaming_quality)}</span>
                </div>
                
                <div class="bg-white rounded-lg shadow-md p-6 mb-6">
                    <h2 class="text-2xl font-bold text-gray-800 mb-4 border-b pb-2">Synopsis</h2>
                    <p class="text-gray-700 leading-relaxed">{html.escape(seo_synopsis)}</p>
                </div>
                
                <div class="flex flex-wrap gap-2 mb-6">
                    <span class="bg-purple-100 text-purple-800 text-xs font-medium px-2.5 py-0.5 rounded-full">Director: {html.escape(director)}</span>
                    {content_advisory_tags}
                </div>
                
                <!-- Watch Now CTA -->
                <div class="bg-gradient-to-r from-blue-500 to-indigo-600 rounded-lg shadow-lg p-6 mb-8 text-center">
                    <h2 class="text-2xl font-bold text-white mb-2">Watch {html.escape(title)} Now!</h2>
                    <p class="text-blue-100 mb-4">Stream this {html.escape(genre)} movie exclusively on ShemarooMe.</p>
                    <a href="{watch_url}" class="inline-block bg-white text-blue-700 font-bold py-3 px-6 rounded-full shadow-md hover:bg-blue-50 transition-colors duration-300" target="_blank">Watch Now</a>
                </div>
            </div>
        </div>
        
        <!-- Cast Section -->
        <section class="mb-12">
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Cast</h2>
            <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-4">
                {cast_html}
            </div>
        </section>
        
        <!-- Reviews Section -->
        <section>
            <h2 class="text-2xl font-bold text-gray-800 mb-6 border-b pb-2">Reviews</h2>
            <div class="space-y-6">
                {reviews_html}
            </div>
        </section>
    </main>
    
    <footer class="bg-gray-800 text-white py-8 px-4 mt-12">
        <div class="container mx-auto max-w-6xl text-center">
            <p>&copy; {time.strftime("%Y")} ShemarooMe. All rights reserved.</p>
            <p class="text-gray-400 text-sm mt-2">Last updated: {last_updated}</p>
        </div>
    </footer>
</body>
</html>
"""
        
        return html_content
    except Exception as e:
        logger.error(f"Error generating blog content: {str(e)}")
        return ""

def generate_index_page(movie_files: List[Path]) -> str:
    """
    Generate an index page listing all movie blog pages using Tailwind CSS.
    
    Args:
        movie_files: List of paths to movie JSON files
        
    Returns:
        HTML content for the index page
    """
    try:
        # Get movie data for all movies
        movies = []
        for movie_file in movie_files:
            movie_data = get_movie_data(movie_file)
            if movie_data and "title" in movie_data and "year" in movie_data:
                movies.append({
                    "title": movie_data["title"],
                    "year": movie_data["year"],
                    "genre": movie_data.get("genre", "Unknown Genre"),
                    "poster_url": movie_data.get("poster_url", ""),
                    "slug": slugify(f"{movie_data['title']}-{movie_data['year']}")
                })
        
        # Sort movies by year (newest first)
        movies.sort(key=lambda x: x["year"], reverse=True)
        
        # Generate movie cards
        movie_cards = ""
        for movie in movies:
            poster_url = movie["poster_url"] if movie["poster_url"] else "https://via.placeholder.com/300x450?text=No+Poster+Available"
            movie_cards += f"""
            <div class="bg-white rounded-lg shadow-md overflow-hidden transition-transform duration-300 hover:scale-105 hover:shadow-xl">
                <a href="{movie['slug']}.html" class="block">
                    {f'<img src="{poster_url}" alt="{html.escape(movie["title"])} Poster" class="w-full h-80 object-cover">' if movie["poster_url"] else '<div class="w-full h-80 bg-gray-200 flex items-center justify-center"><p class="text-gray-500">No Poster Available</p></div>'}
                    <div class="p-4">
                        <h3 class="text-lg font-bold text-gray-800 mb-1">{html.escape(movie['title'])} ({html.escape(str(movie['year']))})</h3>
                        <span class="inline-block bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded-full">{html.escape(movie['genre'])}</span>
                    </div>
                </a>
            </div>
            """
        
        # Create index page HTML with Tailwind CSS
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ShemarooMe Movie Blog</title>
    <meta name="description" content="Explore our collection of movie blogs and reviews on ShemarooMe.">
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Inter', sans-serif;
        }}
    </style>
</head>
<body class="bg-gray-100 text-gray-900">
    <header class="bg-gradient-to-r from-blue-600 to-indigo-700 text-white py-12 px-4 shadow-lg">
        <div class="container mx-auto max-w-6xl text-center">
            <h1 class="text-4xl md:text-5xl font-bold">ShemarooMe Movie Blog</h1>
            <p class="text-xl mt-4 text-blue-100">Explore our collection of movie blogs and reviews</p>
        </div>
    </header>
    
    <main class="container mx-auto max-w-6xl px-4 py-12">
        <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-6">
            {movie_cards}
        </div>
    </main>
    
    <footer class="bg-gray-800 text-white py-8 px-4">
        <div class="container mx-auto max-w-6xl text-center">
            <p>&copy; {time.strftime("%Y")} ShemarooMe. All rights reserved.</p>
            <p class="text-gray-400 text-sm mt-2">Last updated: {time.strftime("%Y-%m-%d")}</p>
        </div>
    </footer>
</body>
</html>
"""
        
        return html_content
    except Exception as e:
        logger.error(f"Error generating index page: {str(e)}")
        return ""

def process_movie_file(movie_file_path: Path) -> None:
    """
    Process a single movie file and generate a blog page.
    
    Args:
        movie_file_path: Path to the movie JSON file
    """
    try:
        # Get movie data
        movie_data = get_movie_data(movie_file_path)
        if not movie_data:
            logger.error(f"Failed to get movie data from {movie_file_path}")
            return
        
        movie_id = movie_data.get("id", "")
        movie_title = movie_data.get("title", "")
        
        if not movie_id or not movie_title:
            logger.warning(f"Missing ID or title in {movie_file_path}")
            return
        
        # Get movie reviews
        reviews = get_movie_reviews(movie_id)
        
        # Generate blog page
        html_content = generate_movie_blog(movie_data, reviews)
        if not html_content:
            logger.error(f"Failed to generate blog content for {movie_title}")
            return
        
        # Create slug for filename
        slug = slugify(f"{movie_title}-{movie_data.get('year', '')}")
        
        # Save blog page
        blog_file_path = BLOGS_DIR / f"{slug}.html"
        with open(blog_file_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Generated blog page for {movie_title} at {blog_file_path}")
        
    except Exception as e:
        logger.error(f"Error processing {movie_file_path}: {str(e)}")

def main() -> None:
    """Main function to process all movie files and generate blog pages."""
    logger.info("Starting movie blog generation process with Tailwind CSS")
    
    # Check if movies directory exists
    if not MOVIES_DIR.exists() or not MOVIES_DIR.is_dir():
        logger.error(f"Movies directory {MOVIES_DIR} does not exist")
        return
    
    # Check if reviews directory exists
    if not REVIEWS_DIR.exists() or not REVIEWS_DIR.is_dir():
        logger.error(f"Reviews directory {REVIEWS_DIR} does not exist")
        return
    
    # Get all movie files
    movie_files = list(MOVIES_DIR.glob("*.json"))
    logger.info(f"Found {len(movie_files)} movie files")
    
    # Process each movie file
    for i, movie_file in enumerate(movie_files):
        logger.info(f"Processing movie {i+1}/{len(movie_files)}: {movie_file.name}")
        process_movie_file(movie_file)
    
    # Generate index page
    index_html = generate_index_page(movie_files)
    if index_html:
        index_file_path = BLOGS_DIR / "index.html"
        with open(index_file_path, 'w') as f:
            f.write(index_html)
        logger.info(f"Generated index page at {index_file_path}")
    
    logger.info("Movie blog generation process completed")

if __name__ == "__main__":
    main()