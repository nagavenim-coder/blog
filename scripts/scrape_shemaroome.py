#!/usr/bin/env python3
"""
Shemaroome Movie Scraper

This script scrapes movie data from shemaroome.com and saves it in JSON format.
Each movie is saved in a separate JSON file with the naming convention <movie_Name>-YYYY.json,
where YYYY is the year of release of the movie.
"""

import os
import json
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import sys
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.shemaroome.com"
MOVIES_URL = f"{BASE_URL}/movies"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "movies")

# More sophisticated headers to avoid 403 errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Referer": "https://www.shemaroome.com/"
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_soup(url, retry_count=3):
    """Get BeautifulSoup object from URL with retries"""
    for attempt in range(retry_count):
        try:
            logger.info(f"Fetching {url} (attempt {attempt+1}/{retry_count})")
            
            # Add some randomization to headers to avoid detection
            modified_headers = HEADERS.copy()
            modified_headers["User-Agent"] = random.choice([
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
            ])
            
            session = requests.Session()
            # First visit the homepage to get cookies
            session.get(BASE_URL, headers=modified_headers, timeout=30)
            # Then visit the actual page
            response = session.get(url, headers=modified_headers, timeout=30)
            response.raise_for_status()
            
            # Check if we got a 403 page
            if "403 ERROR" in response.text or "Access Denied" in response.text:
                logger.error(f"Received 403 error page for {url}")
                if attempt < retry_count - 1:
                    wait_time = (attempt + 1) * 10  # Longer wait for 403s
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    return None
            
            soup = BeautifulSoup(response.text, "html.parser")
            logger.info(f"Successfully fetched {url}")
            return soup
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            if attempt < retry_count - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch {url} after {retry_count} attempts")
                return None

def extract_year(text):
    """Extract year from text using regex"""
    year_match = re.search(r'\b(19\d{2}|20\d{2})\b', text)
    if year_match:
        return year_match.group(1)
    return None

def clean_filename(text):
    """Clean text to be used as filename"""
    # Remove special characters and replace spaces with hyphens
    return re.sub(r'[^\w\s-]', '', text).strip().replace(' ', '-')

def extract_text_after_label(element, label):
    """Extract text after a label in an element"""
    if not element:
        return None
    
    text = element.text.strip()
    # Try to match the label followed by colon or space
    label_pattern = f"({label})[:\\s]+(.*)"
    label_match = re.search(label_pattern, text, re.IGNORECASE)
    if label_match:
        return label_match.group(2).strip()
    
    # If no match with the first pattern, try a more general approach
    # This handles cases where the label might be in a different format
    words = label.split('|')
    for word in words:
        if word.lower() in text.lower():
            # Find the position of the word
            pos = text.lower().find(word.lower())
            # Return everything after the word and any following colon or space
            after_label = text[pos + len(word):].strip()
            # Remove leading colon or space if present
            after_label = re.sub(r'^[:\s]+', '', after_label)
            return after_label
    
    return None

def get_movie_links(soup):
    """Extract movie links from soup object"""
    if not soup:
        return []
        
    movie_links = []
    
    # Try multiple selector patterns to find movie links
    selectors = [
        "a[href*='/movie/']",
        "a[href*='/movies/']",
        ".movie-card a", 
        ".movie-item a", 
        ".movie-thumbnail a",
        ".content-item a",
        ".movie-list a",
        ".movie a"
    ]
    
    for selector in selectors:
        elements = soup.select(selector)
        logger.info(f"Found {len(elements)} elements with selector '{selector}'")
        
        for element in elements:
            href = element.get("href")
            if href and ("/movie/" in href or "/movies/" in href):
                if not href.startswith("http"):
                    href = urljoin(BASE_URL, href)
                movie_links.append(href)
    
    # Remove duplicates
    movie_links = list(set(movie_links))
    logger.info(f"Total unique movie links found: {len(movie_links)}")
    return movie_links

def extract_meta_data(soup):
    """Extract metadata from meta tags"""
    meta_data = {}
    
    # Extract data from meta tags
    meta_tags = soup.find_all('meta')
    for meta in meta_tags:
        name = meta.get('name') or meta.get('property')
        content = meta.get('content')
        
        if name and content:
            if name == 'description':
                meta_data['meta_description'] = content
            elif name == 'keywords':
                meta_data['meta_keywords'] = content
            elif name == 'og:title':
                meta_data['og_title'] = content
            elif name == 'video:release_date':
                meta_data['release_date'] = content
    
    return meta_data

def scrape_movie_details(movie_url):
    """Scrape details from a movie page"""
    soup = get_soup(movie_url)
    if not soup:
        return None
    
    try:
        # Extract metadata from meta tags
        meta_data = extract_meta_data(soup)
        
        # Extract title
        title_element = soup.select_one("h1.movie-title, .movie-details h1, h1")
        title = title_element.text.strip() if title_element else "Unknown Title"
        
        # Check if we got a 403 error page
        if title == "403 ERROR" or "Access Denied" in title:
            logger.error(f"Received 403 error page for {movie_url}")
            return None
        
        # Extract year from meta tags or page content
        year = meta_data.get('release_date', '')
        if year:
            year = extract_year(year)
        
        # Extract data from video-info-left div (as mentioned by user)
        video_info_left = soup.select_one('.video-info-left')
        
        genre = "Unknown"
        language = "Unknown"
        content_rating = "Unknown"
        streaming_quality = "Unknown"
        
        if video_info_left:
            logger.info("Found video-info-left section")
            
            # Extract title if not already found
            if title == "Unknown Title":
                title_element = video_info_left.select_one('h1')
                if title_element:
                    title = title_element.text.strip()
            
            # Extract info from list items
            info_items = video_info_left.select('ul li')
            if info_items:
                for i, item in enumerate(info_items):
                    item_text = item.text.strip()
                    
                    # First item is typically genre
                    if i == 0:
                        genre = item_text
                    
                    # Second item is typically language
                    elif i == 1:
                        language = item_text
                    
                    # Third item is typically year
                    elif i == 2 and not year:
                        year_match = extract_year(item_text)
                        if year_match:
                            year = year_match
                    
                    # Fourth item is typically content rating
                    elif i == 3:
                        content_rating = item_text
                    
                    # Check for streaming quality (4KUHD)
                    if "4KUHD" in item_text or "4K" in item_text or "UHD" in item_text:
                        streaming_quality = item_text
        
        if not year:
            # Try to find year in the page text
            year = extract_year(soup.text)
        
        if not year:
            year = "Unknown"
        
        # Extract movie details from other sections
        details = {}
        detail_items = soup.select(".movie-details-info li, .movie-info li, .details-item")
        
        for item in detail_items:
            item_text = item.text.strip()
            
            # Extract genre if not already found
            if (genre == "Unknown") and ("genre" in item_text.lower() or "category" in item_text.lower()):
                details['genre'] = extract_text_after_label(item, "Genre|Category")
            
            # Extract duration
            if "duration" in item_text.lower() or "runtime" in item_text.lower():
                details['duration'] = extract_text_after_label(item, "Duration|Runtime")
            
            # Extract language if not already found
            if (language == "Unknown") and "language" in item_text.lower():
                details['language'] = extract_text_after_label(item, "Language")
            
            # Extract streaming quality if not already found
            if (streaming_quality == "Unknown") and ("quality" in item_text.lower() or "streaming" in item_text.lower() or "4K" in item_text or "UHD" in item_text):
                details['streaming_quality'] = extract_text_after_label(item, "Quality|Streaming Quality")
            
            # Extract release date if not found in meta tags
            if not year or year == "Unknown":
                if "release" in item_text.lower():
                    release_date = extract_text_after_label(item, "Release Date|Release")
                    if release_date:
                        year_match = extract_year(release_date)
                        if year_match:
                            year = year_match
        
        # Extract synopsis from the specific ID as mentioned by user
        synopsis = "No synopsis available"
        synopsis_element = soup.select_one('#synopsis_data')
        if synopsis_element:
            logger.info("Found synopsis_data element")
            synopsis_text = synopsis_element.text.strip()
            
            # Check if there are "Starring" or "Directed By" sections in the text
            starring_pos = synopsis_text.find("Starring")
            directed_by_pos = synopsis_text.find("Directed By")
            content_advisory_pos = synopsis_text.find("Content Advisory")
            
            # Extract just the synopsis part
            if starring_pos > 0:
                synopsis = synopsis_text[:starring_pos].strip()
            elif directed_by_pos > 0:
                synopsis = synopsis_text[:directed_by_pos].strip()
            elif content_advisory_pos > 0:
                synopsis = synopsis_text[:content_advisory_pos].strip()
            else:
                synopsis = synopsis_text
        else:
            # Fallback to other selectors if #synopsis_data is not found
            fallback_synopsis = soup.select_one(".synopsis-text, .plot, .description, .synopsis, .movie-description")
            if fallback_synopsis:
                synopsis = fallback_synopsis.text.strip()
        
        # Extract cast from synopsis_data
        cast = []
        if synopsis_element:
            full_text = synopsis_element.text.strip()
            starring_match = re.search(r'Starring\s+(.*?)(?:Directed By|Content Advisory|$)', full_text, re.DOTALL)
            if starring_match:
                cast_text = starring_match.group(1).strip()
                # Split by commas or other separators
                cast = [actor.strip() for actor in re.split(r',|\|', cast_text) if actor.strip()]
        
        if not cast:
            # Fallback to other selectors
            cast_element = soup.select_one(".starring-text, .cast, .actors")
            if cast_element:
                cast_text = cast_element.text.strip()
                # Remove "Starring:" or similar labels
                cast_text = re.sub(r'^(Starring|Cast|Actors)[:\s]+', '', cast_text, flags=re.IGNORECASE)
                # Split by commas or other separators
                cast = [actor.strip() for actor in re.split(r',|\|', cast_text) if actor.strip()]
        
        # Extract director from synopsis_data
        director = "Unknown"
        if synopsis_element:
            full_text = synopsis_element.text.strip()
            director_match = re.search(r'Directed By\s+(.*?)(?:Content Advisory|$)', full_text, re.DOTALL)
            if director_match:
                director = director_match.group(1).strip()
        
        if director == "Unknown":
            # Fallback to other selectors
            director_element = soup.select_one(".director-text, .director")
            if director_element:
                director_text = director_element.text.strip()
                # Remove "Directed by:" or similar labels
                director = re.sub(r'^(Directed by|Director)[:\s]+', '', director_text, flags=re.IGNORECASE).strip()
        
        # Extract content advisory from synopsis_data
        content_advisory = ""
        if synopsis_element:
            full_text = synopsis_element.text.strip()
            advisory_match = re.search(r'Content Advisory\s+(.*?)$', full_text, re.DOTALL)
            if advisory_match:
                content_advisory = advisory_match.group(1).strip()
        
        # Extract poster URL
        poster_url = None
        poster_element = soup.select_one(".movie-poster img, .poster img, .thumbnail img, .video-thumb img")
        if poster_element:
            poster_url = poster_element.get("src") or poster_element.get("data-src")
            if poster_url and not poster_url.startswith("http"):
                poster_url = urljoin(BASE_URL, poster_url)
        
        # Extract trailer URL
        trailer_url = None
        trailer_element = soup.select_one("a.trailer-link, .watch-trailer, a[href*='trailer']")
        if trailer_element:
            trailer_url = trailer_element.get("href")
            if trailer_url and not trailer_url.startswith("http"):
                trailer_url = urljoin(BASE_URL, trailer_url)
        
        # Extract recommendations (You may like)
        recommendations = []
        
        # Try multiple selectors for recommendations
        recommendation_selectors = [
            ".you-may-like-slider .item", 
            ".you-may-also-like .item", 
            ".recommendations .item", 
            ".similar-movies .item",
            ".you-may-like .item",
            ".related-content .item",
            ".more-like-this .item"
        ]
        
        for selector in recommendation_selectors:
            recommendation_items = soup.select(selector)
            if recommendation_items:
                logger.info(f"Found {len(recommendation_items)} recommendation items with selector '{selector}'")
                break
        
        # If we found recommendations, extract their details
        for item in recommendation_items[:5]:  # Limit to 5 recommendations
            rec_title_element = item.select_one(".title, h3, .movie-title, .content-title")
            rec_link_element = item.select_one("a")
            
            if rec_title_element and rec_link_element:
                rec_title = rec_title_element.text.strip()
                rec_link = rec_link_element.get("href")
                if rec_link and not rec_link.startswith("http"):
                    rec_link = urljoin(BASE_URL, rec_link)
                
                rec_poster = None
                rec_poster_element = item.select_one("img")
                if rec_poster_element:
                    rec_poster = rec_poster_element.get("src") or rec_poster_element.get("data-src")
                    if rec_poster and not rec_poster.startswith("http"):
                        rec_poster = urljoin(BASE_URL, rec_poster)
                
                recommendations.append({
                    "title": rec_title,
                    "url": rec_link,
                    "poster": rec_poster
                })
        
        # Extract duration from video player if available
        duration = details.get('duration', "Unknown")
        duration_element = soup.select_one(".duration-time, .video-duration")
        if duration_element:
            duration = duration_element.text.strip()
        
        # Generate a unique ID
        movie_id = re.search(r'/movies?/([^/]+)', movie_url)
        if movie_id:
            movie_id = movie_id.group(1)
        else:
            movie_id = str(hash(title))
        
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Create movie data dictionary with all extracted information
        movie_data = {
            "id": movie_id,
            "title": title,
            "year": year,
            "genre": genre if genre != "Unknown" else details.get('genre', "Unknown"),
            "duration": duration,
            "language": language if language != "Unknown" else details.get('language', "Unknown"),
            "content_rating": content_rating,
            "streaming_quality": streaming_quality if streaming_quality != "Unknown" else details.get('streaming_quality', "Unknown"),
            "plot": synopsis,
            "cast": cast,
            "director": director,
            "content_advisory": content_advisory,
            "poster_url": poster_url,
            "trailer_url": trailer_url,
            "watch_url": movie_url,
            "recommendations": recommendations,
            "meta_description": meta_data.get('meta_description', ""),
            "meta_keywords": meta_data.get('meta_keywords', ""),
            "scrapped_date": current_date,
            "last_updated": current_date
        }
        
        logger.info(f"Successfully scraped data for movie: {title}")
        return movie_data
    
    except Exception as e:
        logger.error(f"Error scraping movie details from {movie_url}: {e}", exc_info=True)
        return None

def save_movie_data(movie_data):
    """Save movie data to JSON file"""
    if not movie_data or not movie_data.get("title"):
        logger.error("Missing required data for saving movie")
        return False
    
    # Use "Unknown" for year if not available
    year = movie_data.get("year", "Unknown")
    if year == "Unknown":
        logger.warning(f"Using 'Unknown' as year for movie {movie_data['title']}")
    
    filename = f"{clean_filename(movie_data['title'])}-{year}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(movie_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving {filename}: {e}", exc_info=True)
        return False

def explore_site():
    """Explore the site structure to find movie pages"""
    logger.info(f"Starting site exploration from {BASE_URL}")
    
    # Start with the main page
    main_soup = get_soup(BASE_URL)
    if not main_soup:
        logger.error("Failed to fetch main page")
        return []
    
    # Find all navigation links that might lead to movie listings
    nav_links = []
    potential_nav_selectors = [
        "nav a", 
        ".navigation a", 
        ".menu a", 
        ".main-menu a",
        "#header a",
        ".navbar a",
        ".categories a",
        ".genres a"
    ]
    
    for selector in potential_nav_selectors:
        elements = main_soup.select(selector)
        logger.info(f"Found {len(elements)} navigation elements with selector '{selector}'")
        
        for element in elements:
            href = element.get("href")
            if href:
                if not href.startswith("http") and not href.startswith("#"):
                    href = urljoin(BASE_URL, href)
                if href.startswith(BASE_URL):  # Only include internal links
                    nav_links.append(href)
    
    # Add movies page explicitly
    nav_links.append(MOVIES_URL)
    
    # Remove duplicates
    nav_links = list(set(nav_links))
    logger.info(f"Found {len(nav_links)} unique navigation links")
    
    # Explore each navigation link to find movie links
    all_movie_links = []
    visited_pages = set()
    
    for nav_url in nav_links[:10]:  # Limit to first 10 links to avoid too many requests
        if nav_url in visited_pages:
            continue
            
        visited_pages.add(nav_url)
        logger.info(f"Exploring navigation link: {nav_url}")
        
        nav_soup = get_soup(nav_url)
        if not nav_soup:
            continue
        
        # Extract movie links from this page
        movie_links = get_movie_links(nav_soup)
        all_movie_links.extend(movie_links)
        
        # Be nice to the server
        time.sleep(random.uniform(2, 5))
    
    # Remove duplicates
    all_movie_links = list(set(all_movie_links))
    logger.info(f"Total unique movie links found across site: {len(all_movie_links)}")
    
    return all_movie_links

def scrape_specific_movie(url):
    """Scrape a specific movie URL"""
    logger.info(f"Scraping specific movie: {url}")
    movie_data = scrape_movie_details(url)
    if movie_data:
        if save_movie_data(movie_data):
            logger.info(f"Successfully scraped and saved movie: {movie_data['title']}")
            return True
    logger.error(f"Failed to scrape movie from {url}")
    return False

def main():
    """Main function to scrape movies"""
    logger.info(f"Starting Shemaroome movie scraper")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    
    # Check if a specific URL was provided as a command line argument
    if len(sys.argv) > 1 and sys.argv[1].startswith("http"):
        return scrape_specific_movie(sys.argv[1])
    
    # First try to explore the site structure
    movie_links = explore_site()
    
    if not movie_links:
        logger.warning("No movie links found through site exploration. Trying direct movie page")
        
        # If we couldn't find any movies through exploration, try the movies page directly
        movies_soup = get_soup(MOVIES_URL)
        if movies_soup:
            movie_links = get_movie_links(movies_soup)
    
    if not movie_links:
        logger.error("Failed to find any movie links. Exiting.")
        return False
    
    logger.info(f"Found {len(movie_links)} movie links to scrape")
    
    # Scrape each movie
    successful_scrapes = 0
    for i, movie_url in enumerate(movie_links):
        logger.info(f"Scraping movie {i+1}/{len(movie_links)}: {movie_url}")
        movie_data = scrape_movie_details(movie_url)
        if movie_data:
            if save_movie_data(movie_data):
                successful_scrapes += 1
        
        # Be nice to the server
        time.sleep(random.uniform(3, 7))
    
    logger.info(f"Scraping completed. Successfully scraped {successful_scrapes} out of {len(movie_links)} movies.")
    return successful_scrapes > 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
