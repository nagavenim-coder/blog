# Shemaroome Movie Scraper

This script scrapes movie data from shemaroome.com and saves it in JSON format.

## Features

- Scrapes movie details from shemaroome.com
- Saves each movie in a separate JSON file with the naming convention `<movie_Name>-YYYY.json`
- Collects the following information for each movie:
  1. id
  2. title
  3. year
  4. genre
  5. duration
  6. plot
  7. cast
  8. director
  9. poster_url
  10. trailer_url
  11. watch_url
  12. scrapped_date
  13. last_updated

## Usage

```bash
python3 scripts/scrape_shemaroome.py
```

## Output

The script saves JSON files in the `data/movies` directory. Each file is named according to the pattern `<movie_Name>-YYYY.json`, where `YYYY` is the year of release of the movie.

## Notes

- The script includes random delays between requests to avoid overloading the server
- It handles various HTML structures that might be present on the website
- Error handling is included to prevent the script from crashing if a movie page has unexpected structure

## Requirements

- Python 3.6+
- Required packages: requests, beautifulsoup4, datetime
