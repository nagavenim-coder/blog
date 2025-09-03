#!/usr/bin/env python3
"""
Web Plot Scraper using Serper API

This script searches for movie plots using Serper API and extracts detailed synopsis.
"""

import json
import ssl
import http.client
import requests
from bs4 import BeautifulSoup
import time
import random
from pathlib import Path
import logging
import boto3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='web_plot_scraper.log'
)

# Constants
BASE_DIR = Path(__file__).parent.parent
MOVIES_DIR = BASE_DIR / "data/movies"
SERPER_API_KEY = "YOUR_SERPER_API_KEY_HERE"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-5-haiku-20241022-v1:0"

def search_with_serper(query):
    """Search using Serper API."""
    try:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        conn = http.client.HTTPSConnection("google.serper.dev", context=context)
        payload = json.dumps({"q": query})
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        conn.request("POST", "/search", payload, headers)
        res = conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        logging.error(f"Serper API error: {e}")
        return None

def extract_plot_from_url(url):
    """Extract plot content from a URL."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10, verify=False)
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
        
        return plot_text.strip() if len(plot_text.strip()) >= 300 else None
        
    except Exception as e:
        logging.error(f"Error extracting from {url}: {e}")
        return None

def extract_plot_with_bedrock(raw_text, movie_title, year, existing_plot):
    """Extract clean plot summary using AWS Bedrock Claude."""
    try:
        bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
        
        prompt = f"""Extract and create a detailed movie plot summary for "{movie_title} ({year})".

Existing plot: {existing_plot}

Source text: {raw_text}

If the source text contains relevant plot information, create a detailed 400-500 word plot summary. Return ONLY the plot summary without any explanations, analysis, or commentary.

If the text is irrelevant (cookies, ads, unrelated content), respond with: "NO_PLOT_FOUND"

Plot Summary:"""
        
        request_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "temperature": 0.3,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_payload)
        )
        
        response_body = json.loads(response.get('body').read())
        clean_plot = response_body.get('content', [{}])[0].get('text', '').strip()
        
        if "NO_PLOT_FOUND" in clean_plot:
            return None
        
        # Clean unwanted characters and encoding issues
        import re
        clean_plot = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', clean_plot)  # Remove control characters
        clean_plot = re.sub(r'\\[nrt]', ' ', clean_plot)  # Remove escape sequences
        clean_plot = re.sub(r'[^\w\s.,!?;:()\-\'"]+', ' ', clean_plot)  # Keep only readable characters
        clean_plot = re.sub(r'\s+', ' ', clean_plot).strip()  # Normalize whitespace
        
        return clean_plot if len(clean_plot) >= 300 else None
        
    except Exception as e:
        logging.error(f"Bedrock error for {movie_title}: {e}")
        return None

def search_movie_plot(movie_title, year):
    """Search for movie plot using Serper API."""
    queries = [
        f"{movie_title} {year} plot summary wikipedia",
        f"{movie_title} {year} movie synopsis",
        f"{movie_title} {year} story plot detailed"
    ]
    
    for query in queries:
        try:
            search_results = search_with_serper(query)
            if not search_results or 'organic' not in search_results:
                continue
            
            for result in search_results['organic'][:10]:
                url = result.get('link', '')
                if not any(site in url for site in ['imdb.com', 'netflix.com', 'amazon.com', 'hulu.com', 'disney.com']):
                    plot = extract_plot_from_url(url)
                    if plot and len(plot) >= 500:
                        logging.info(f"Found plot for {movie_title} from {url}")
                        return plot
            
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            logging.error(f"Error searching for {movie_title}: {e}")
    
    return None

def update_movie_plot(movie_file_path):
    """Update movie plot with web-scraped content."""
    try:
        with open(movie_file_path, 'r', encoding='utf-8') as f:
            movie_data = json.load(f)
        
        title = movie_data.get('title', '')
        year = movie_data.get('year', '')
        current_plot = movie_data.get('plot', '')
        
        if len(current_plot) >= 500:
            logging.info(f"Skipping {title} - already has detailed plot")
            return
        
        logging.info(f"Searching web plot for {title} ({year})")
        
        web_plot = search_movie_plot(title, year)
        
        if web_plot and len(web_plot) >= 500:
            clean_plot = extract_plot_with_bedrock(web_plot, title, year, current_plot)
            if clean_plot:
                movie_data['web_plot'] = clean_plot
                movie_data['raw_web_plot'] = web_plot
                movie_data['plot_source'] = 'web_scraped_bedrock_cleaned'
            else:
                movie_data['web_plot'] = web_plot
                movie_data['plot_source'] = 'web_scraped'
            movie_data['last_updated'] = time.strftime('%Y-%m-%d')
            
            with open(movie_file_path, 'w', encoding='utf-8') as f:
                json.dump(movie_data, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Updated plot for {title} ({len(web_plot)} characters)")
        else:
            logging.warning(f"Could not find detailed plot for {title}")
        
        time.sleep(random.uniform(2, 4))
        
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
        update_movie_plot(movie_file)

if __name__ == "__main__":
    main()