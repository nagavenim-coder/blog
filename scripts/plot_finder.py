"""
Plot Finder Script for Movie Blog Generator

This script searches for detailed movie plots using Serper API and updates movie data.
Can be run standalone or integrated with other scripts like scrape_shemaroome.py.
"""

import os
import json
import ssl
import http.client
import requests
from bs4 import BeautifulSoup
import time
import random
import re
import boto3
import logging
import sys
import warnings
from pathlib import Path
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("plot_finder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Reduce noise from other libraries
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

class PlotFinder:
    """Tool for finding detailed movie plots using Serper API."""
    
    def __init__(self, serper_api_key=None, aws_region="us-east-1", bedrock_model_id="anthropic.claude-3-sonnet-20240229-v1:0"):
        """Initialize the plot finder tool."""
        self.serper_api_key = serper_api_key or os.getenv("67d87a4af42c77c8990778dde14f5d051e0464b2")
        self.aws_region = aws_region
        self.bedrock_model_id = bedrock_model_id
        
        if not self.serper_api_key:
            logger.warning("No Serper API key provided. Plot search will use simulated data.")
        
        # Set up paths
        self.base_dir = Path(__file__).parent.parent
        self.movies_dir = self.base_dir / "data" / "movies"
    
    def process_movie_file(self, movie_file_path):
        """
        Process a single movie JSON file to enhance its plot.
        
        Args:
            movie_file_path: Path to the movie JSON file
                
        Returns:
            dict: Result of the plot finding operation
        """
        try:
            # Load movie data
            with open(movie_file_path, 'r', encoding='utf-8') as f:
                movie_data = json.load(f)
            
            movie_title = movie_data.get("title")
            if not movie_title:
                logger.error(f"No title found in {movie_file_path}")
                return {"success": False, "error": "No movie title found"}
            
            # Check if plot is already adequate
            current_plot = movie_data.get("plot", "")
            web_plot = movie_data.get("web_plot", "")
            
            # Check if existing web plot is valid
            if len(web_plot) >= 400 and self._is_valid_plot_content(web_plot):
                logger.info(f"Plot for {movie_title} is already enhanced")
                return {"success": True, "movie_title": movie_title, "status": "existing"}
            elif web_plot and not self._is_valid_plot_content(web_plot):
                logger.info(f"Existing web plot for {movie_title} is invalid, will re-enhance")
                # Clear the invalid plot
                movie_data["web_plot"] = ""
                movie_data["plot_source"] = ""
            
            # Skip if current plot is already very detailed
            if len(current_plot) >= 800:
                logger.info(f"Current plot for {movie_title} is already detailed enough")
                return {"success": True, "movie_title": movie_title, "status": "adequate"}
            
            # Find detailed plot
            year = movie_data.get("year", "")
            enhanced_plot = self._search_movie_plot(movie_title, year)
            
            if not enhanced_plot:
                logger.warning(f"No valid plot found for {movie_title}")
                return {"success": False, "error": "No valid plot found", "movie_title": movie_title}
            
            if len(enhanced_plot) < 200:
                logger.warning(f"Found plot for {movie_title} is too short ({len(enhanced_plot)} chars)")
                return {"success": False, "error": "Plot too short", "movie_title": movie_title}
            
            # Clean plot with Bedrock if available
            clean_plot = self._extract_plot_with_bedrock(enhanced_plot, movie_title, year, current_plot)
            
            # Update movie data with new plot
            if clean_plot and len(clean_plot) >= 300:
                movie_data["web_plot"] = clean_plot
                movie_data["raw_web_plot"] = enhanced_plot
                movie_data["plot_source"] = "web_scraped_bedrock_cleaned"
                logger.info(f"Enhanced plot for {movie_title} with Bedrock cleaning")
            elif enhanced_plot:
                movie_data["web_plot"] = enhanced_plot
                movie_data["plot_source"] = "web_scraped"
                logger.info(f"Enhanced plot for {movie_title} with web scraping")
            else:
                return {"success": False, "error": "No suitable plot found"}
            
            movie_data["last_updated"] = time.strftime("%Y-%m-%d")
            
            # Save updated movie data
            with open(movie_file_path, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, ensure_ascii=False, indent=2)
            
            return {
                "success": True,
                "movie_title": movie_title,
                "status": "updated",
                "plot_length": len(movie_data.get("web_plot", ""))
            }
        except Exception as e:
            logger.error(f"Error processing movie file {movie_file_path}: {e}")
            return {"success": False, "error": str(e)}
    
    def process_all_movies(self):
        """Process all movie JSON files in the movies directory."""
        if not self.movies_dir.exists():
            logger.error(f"Movies directory not found: {self.movies_dir}")
            return False
        
        movie_files = list(self.movies_dir.glob("*.json"))
        if not movie_files:
            logger.error(f"No JSON files found in {self.movies_dir}")
            return False
        
        logger.info(f"Found {len(movie_files)} movie files to process")
        
        successful_updates = 0
        skipped_files = 0
        failed_files = 0
        
        for i, movie_file in enumerate(movie_files):
            logger.info(f"Processing {i+1}/{len(movie_files)}: {movie_file.name}")
            
            result = self.process_movie_file(movie_file)
            
            if result["success"]:
                if result["status"] == "updated":
                    successful_updates += 1
                else:
                    skipped_files += 1
            else:
                failed_files += 1
                error_msg = result.get('error', 'Unknown error')
                movie_title = result.get('movie_title', movie_file.stem)
                if error_msg in ["No valid plot found", "Plot too short"]:
                    logger.info(f"No suitable plot found for {movie_title}")
                else:
                    logger.error(f"Failed to process {movie_title}: {error_msg}")
            
            # Be nice to APIs - add delay between requests
            time.sleep(random.uniform(2, 4))
        
        logger.info(f"Processing completed. Updated: {successful_updates}, Skipped: {skipped_files}, Failed: {failed_files}")
        return successful_updates > 0
    
    def _search_with_serper(self, query):
        """Search using Serper API."""
        if not self.serper_api_key:
            logger.warning("No Serper API key available")
            return None
            
        try:
            # Use requests instead of http.client for better SSL handling
            url = "https://google.serper.dev/search"
            payload = {"q": query}
            headers = {
                'X-API-KEY': self.serper_api_key,
                'Content-Type': 'application/json'
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=30, verify=True)
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Serper API error: {e}")
            return None
    
    def _is_valid_url(self, url):
        """Check if URL is valid and safe to process."""
        if not url or not url.startswith(('http://', 'https://')):
            return False
        
        # Skip problematic domains
        problematic_domains = [
            'revionz.safariinfosoft.com',
            'facebook.com', 'instagram.com', 'twitter.com', 'youtube.com',
            'linkedin.com', 'pinterest.com', 'tiktok.com', 'snapchat.com'
        ]
        
        return not any(domain in url.lower() for domain in problematic_domains)
    
    def _is_valid_plot_content(self, text):
        """Check if extracted text is valid movie plot content."""
        if not text or len(text.strip()) < 100:
            return False
        
        text_lower = text.lower()
        
        # Check for security/error messages
        security_indicators = [
            'cloudflare', 'security service', 'online attacks', 'ray id',
            'blocked', 'triggered the security solution', 'sql command',
            'malformed data', 'performance & security by',
            'this website is using a security service',
            'you were blocked', 'click to reveal', 'your ip'
        ]
        
        # Check for common error pages
        error_indicators = [
            '404 not found', '403 forbidden', '500 internal server error',
            'page not found', 'access denied', 'server error',
            'temporarily unavailable', 'under maintenance'
        ]
        
        # Check for cookie/privacy notices
        privacy_indicators = [
            'cookie policy', 'privacy policy', 'gdpr', 'accept cookies',
            'we use cookies', 'consent', 'data protection',
            'terms of service', 'privacy notice'
        ]
        
        # Check for subscription/advertisement content
        commercial_indicators = [
            'subscribe now', 'sign up', 'create account', 'login required',
            'premium content', 'advertisement', 'sponsored content',
            'newsletter', 'follow us', 'social media'
        ]
        
        # Check for navigation/UI elements
        ui_indicators = [
            'home page', 'contact us', 'about us', 'search results',
            'navigation menu', 'sidebar', 'footer', 'header',
            'click here', 'read more', 'load more'
        ]
        
        all_indicators = (security_indicators + error_indicators + 
                         privacy_indicators + commercial_indicators + ui_indicators)
        
        # If text contains multiple indicators, it's likely not a plot
        indicator_count = sum(1 for indicator in all_indicators if indicator in text_lower)
        if indicator_count >= 3:
            logger.debug(f"Text rejected: contains {indicator_count} non-plot indicators")
            return False
        
        # Check for specific problematic phrases
        if any(indicator in text_lower for indicator in security_indicators[:5]):
            logger.debug("Text rejected: contains security/error content")
            return False
        
        # Check for generic simulated plot patterns
        simulated_patterns = [
            'follows the journey of a protagonist who faces numerous challenges',
            'the film begins with an introduction to the main character',
            'inciting incident disrupts their routine',
            'explores themes of perseverance, identity, and the human condition',
            'offering viewers a compelling story that resonates on multiple levels'
        ]
        
        if any(pattern in text_lower for pattern in simulated_patterns):
            logger.debug("Text rejected: contains generic simulated plot content")
            return False
        
        # Check if text is mostly navigation or UI elements
        sentences = text.split('.')
        if len(sentences) < 3:  # Very short content, likely not a plot
            return False
        
        # Check for movie-related keywords to confirm it's about a film
        movie_keywords = [
            'film', 'movie', 'story', 'plot', 'character', 'protagonist',
            'drama', 'comedy', 'thriller', 'romance', 'action',
            'directed', 'starring', 'cast', 'screenplay', 'cinema'
        ]
        
        has_movie_keywords = any(keyword in text_lower for keyword in movie_keywords)
        
        # Require substantial content with movie keywords
        if len(text) >= 400 and has_movie_keywords and indicator_count == 0:
            return True
        
        # For preferred sources, allow slightly shorter content
        if len(text) >= 300 and has_movie_keywords and indicator_count == 0:
            return True
        
        return False
    
    def _extract_plot_from_url(self, url):
        """Extract plot content from a URL."""
        if not self._is_valid_url(url):
            logger.debug(f"Skipping invalid or problematic URL: {url}")
            return None
            
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Use session for better connection handling
            session = requests.Session()
            session.headers.update(headers)
            
            # Set reasonable timeouts
            timeout = (10, 20)  # (connect timeout, read timeout)
            
            # Try with SSL verification first, fall back to unverified if needed
            try:
                response = session.get(url, timeout=timeout, verify=True)
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                logger.debug(f"SSL/Connection error for {url}, trying without verification")
                try:
                    response = session.get(url, timeout=timeout, verify=False)
                except requests.exceptions.Timeout:
                    logger.warning(f"Timeout accessing {url}")
                    return None
            
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                logger.debug(f"Skipping non-HTML content from {url}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            plot_text = ""
            
            # Wikipedia extraction
            if 'wikipedia.org' in url:
                for heading in soup.find_all(['h2', 'h3']):
                    if any(word in heading.get_text().lower() for word in ['plot', 'synopsis', 'story']):
                        current = heading.find_next_sibling()
                        while current and current.name not in ['h2', 'h3']:
                            if current.name == 'p':
                                plot_text += current.get_text().strip() + " "
                            current = current.find_next_sibling()
                        break
            
            # General extraction for public sites
            else:
                paragraphs = soup.find_all('p')
                for p in paragraphs[:15]:  # First 15 paragraphs
                    text = p.get_text().strip()
                    if len(text) > 50 and not any(word in text.lower() for word in ['cookie', 'privacy', 'consent', 'advertisement', 'subscribe', 'newsletter']):
                        plot_text += text + " "
            
            plot_text = plot_text.strip()
            
            # Validate the extracted content
            if self._is_valid_plot_content(plot_text):
                return plot_text
            else:
                logger.debug(f"Extracted content from {url} failed validation")
                return None
            
        except Exception as e:
            logger.error(f"Error extracting from {url}: {e}")
            return None
    
    def _extract_plot_with_bedrock(self, raw_text, movie_title, year, existing_plot):
        """Extract clean plot summary using AWS Bedrock Claude."""
        try:
            bedrock = boto3.client('bedrock-runtime', region_name=self.aws_region)
            
            prompt = f"""Extract and create a detailed movie plot summary for "{movie_title} ({year})".

Existing plot: {existing_plot}

Source text: {raw_text}

IMPORTANT: Only extract if the source text contains actual movie plot information. 

IGNORE and respond with "NO_PLOT_FOUND" if the text contains:
- Security messages (Cloudflare, blocked access, Ray ID)
- Error pages (404, 403, server errors)
- Cookie/privacy notices
- Advertisement or subscription content
- Website navigation elements
- Any non-movie related content

If the source text contains relevant plot information, create a detailed 400-500 word plot summary. Return ONLY the plot summary without any explanations, analysis, or commentary.

Plot Summary:"""
            
            request_payload = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "temperature": 0.3,
                "messages": [{"role": "user", "content": prompt}]
            }
            
            response = bedrock.invoke_model(
                modelId=self.bedrock_model_id,
                body=json.dumps(request_payload)
            )
            
            response_body = json.loads(response.get('body').read())
            clean_plot = response_body.get('content', [{}])[0].get('text', '').strip()
            
            if "NO_PLOT_FOUND" in clean_plot:
                return None
            
            # Clean unwanted characters and encoding issues
            clean_plot = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', clean_plot)  # Remove control characters
            clean_plot = re.sub(r'\\[nrt]', ' ', clean_plot)  # Remove escape sequences
            clean_plot = re.sub(r'[^\w\s.,!?;:()\-\'"]+', ' ', clean_plot)  # Keep only readable characters
            clean_plot = re.sub(r'\s+', ' ', clean_plot).strip()  # Normalize whitespace
            
            return clean_plot if len(clean_plot) >= 300 else None
            
        except Exception as e:
            logger.error(f"Bedrock error for {movie_title}: {e}")
            return None
    
    def _search_movie_plot(self, movie_title, year):
        """Search for movie plot using Serper API."""

        
        queries = [
            f"{movie_title} {year} plot summary wikipedia",
            f"{movie_title} {year} movie synopsis",
            f"{movie_title} {year} story plot detailed"
        ]
        
        for query in queries:
            try:
                search_results = self._search_with_serper(query)
                if not search_results or 'organic' not in search_results:
                    continue
                
                for result in search_results['organic'][:10]:
                    url = result.get('link', '')
                    
                    # Skip problematic sites and social media
                    skip_sites = [
                        'imdb.com', 'netflix.com', 'amazon.com', 'hulu.com', 'disney.com',
                        'facebook.com', 'instagram.com', 'twitter.com', 'youtube.com',
                        'linkedin.com', 'pinterest.com', 'tiktok.com'
                    ]
                    
                    if not any(site in url.lower() for site in skip_sites):
                        # Prefer reliable sources
                        preferred_sites = ['wikipedia.org', 'britannica.com', 'imdb.com']
                        is_preferred = any(site in url.lower() for site in preferred_sites)
                        
                        plot = self._extract_plot_from_url(url)
                        if plot and len(plot) >= 400:  # Require substantial content
                            logger.info(f"Found plot for {movie_title} from {url}")
                            return plot
                        
                        # If it's a preferred site, allow slightly shorter content
                        elif plot and len(plot) >= 300 and is_preferred:
                            logger.info(f"Found plot for {movie_title} from preferred source {url}")
                            return plot
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error searching for {movie_title}: {e}")
        
        # If we couldn't find a plot, return None
        logger.warning(f"Could not find a valid plot for {movie_title} ({year})")
        return None
    


def enhance_movie_plots_from_directory(movies_dir, serper_api_key=None):
    """
    Enhance plots for all movies in a directory.
    This function can be called from other scripts like scrape_shemaroome.py
    
    Args:
        movies_dir: Path to directory containing movie JSON files
        serper_api_key: Optional Serper API key
    
    Returns:
        dict: Summary of processing results
    """
    plot_finder = PlotFinder(serper_api_key=serper_api_key)
    plot_finder.movies_dir = Path(movies_dir)
    
    if not plot_finder.movies_dir.exists():
        logger.error(f"Movies directory not found: {movies_dir}")
        return {"success": False, "error": "Movies directory not found"}
    
    movie_files = list(plot_finder.movies_dir.glob("*.json"))
    if not movie_files:
        logger.error(f"No JSON files found in {movies_dir}")
        return {"success": False, "error": "No movie files found"}
    
    logger.info(f"Enhancing plots for {len(movie_files)} movies")
    
    results = {
        "total_files": len(movie_files),
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "success": True
    }
    
    for movie_file in movie_files:
        result = plot_finder.process_movie_file(movie_file)
        
        if result["success"]:
            if result["status"] == "updated":
                results["updated"] += 1
            else:
                results["skipped"] += 1
        else:
            results["failed"] += 1
        
        # Rate limiting
        time.sleep(random.uniform(1, 3))
    
    logger.info(f"Plot enhancement completed. Updated: {results['updated']}, Skipped: {results['skipped']}, Failed: {results['failed']}")
    return results

def clean_invalid_plots(movies_dir):
    """Clean up movies that have invalid web plots."""
    movies_dir = Path(movies_dir)
    if not movies_dir.exists():
        logger.error(f"Movies directory not found: {movies_dir}")
        return False
    
    plot_finder = PlotFinder()
    movie_files = list(movies_dir.glob("*.json"))
    cleaned_count = 0
    
    for movie_file in movie_files:
        try:
            with open(movie_file, 'r', encoding='utf-8') as f:
                movie_data = json.load(f)
            
            web_plot = movie_data.get("web_plot", "")
            if web_plot and not plot_finder._is_valid_plot_content(web_plot):
                logger.info(f"Cleaning invalid plot from {movie_data.get('title', 'Unknown')}")
                movie_data["web_plot"] = ""
                movie_data["plot_source"] = ""
                movie_data["last_updated"] = time.strftime("%Y-%m-%d")
                
                with open(movie_file, 'w', encoding='utf-8') as f:
                    json.dump(movie_data, f, ensure_ascii=False, indent=2)
                
                cleaned_count += 1
        except Exception as e:
            logger.error(f"Error cleaning {movie_file}: {e}")
    
    logger.info(f"Cleaned {cleaned_count} movies with invalid plots")
    return cleaned_count > 0

def main():
    """Main function to run plot finder as standalone script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhance movie plots using web scraping and AI")
    parser.add_argument("--movies-dir", default=None, help="Directory containing movie JSON files")
    parser.add_argument("--serper-key", default=None, help="Serper API key for web search")
    parser.add_argument("--single-file", default=None, help="Process a single movie JSON file")
    parser.add_argument("--clean-invalid", action="store_true", help="Clean up movies with invalid plots")
    
    args = parser.parse_args()
    
    # Set up default movies directory
    if not args.movies_dir:
        base_dir = Path(__file__).parent.parent
        args.movies_dir = base_dir / "data" / "movies"
    
    plot_finder = PlotFinder(serper_api_key=args.serper_key)
    plot_finder.movies_dir = Path(args.movies_dir)
    
    if args.clean_invalid:
        # Clean up invalid plots
        logger.info("Cleaning up movies with invalid plots...")
        success = clean_invalid_plots(args.movies_dir)
        return success
    elif args.single_file:
        # Process single file
        logger.info(f"Processing single file: {args.single_file}")
        result = plot_finder.process_movie_file(Path(args.single_file))
        if result["success"]:
            logger.info(f"Successfully processed {args.single_file}")
            return True
        else:
            logger.error(f"Failed to process {args.single_file}: {result.get('error')}")
            return False
    else:
        # Process all files
        success = plot_finder.process_all_movies()
        return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
