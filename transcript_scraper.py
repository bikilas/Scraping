import socket
import requests
from datetime import datetime, timedelta
import time
import pytz
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional, Tuple, Any
import json
from pathlib import Path
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
# import random

class TranscriptScraper:
    def __init__(self, config_path: str = 'scraper_config.json'):
        """Initialize the scraper with configuration."""
        HOST = '0.0.0.0'   # Listen on all network interfaces
        PORT = 5000        # Port number
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((HOST, PORT))
        server.listen(1)

        print(f"Listening on port {PORT}...")

        conn, addr = server.accept()
        print(f"Connected by {addr}")

        conn.sendall(b"Hello from server!")
        conn.close()
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.EST = pytz.timezone('America/New_York')
        self.session = self._create_session()
        self._last_request_time = 0
        self.logger.info("TranscriptScraper initialized")

    def _setup_logging(self) -> logging.Logger:
        """Configure logging based on config."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        if logger.hasHandlers():
            logger.handlers.clear()
            
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # File handler
        log_config = self.config.get('logging', {})
        file_handler = logging.FileHandler('transcript_scraper.log')
        file_handler.setLevel(getattr(logging, log_config.get('level', 'INFO')))
        file_handler.setFormatter(formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        return logger

    def _create_session(self) -> requests.Session:
        """Create and configure a requests session with retries."""
        session = requests.Session()
        site_config = next(iter(self.config.get('sites', {}).values()), {})
        
        # Update headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1'
        }
        headers.update(site_config.get('headers', {}))
        session.headers.update(headers)
        
        # Configure retries
        retry_strategy = Retry(
            total=site_config.get('retry_attempts', 3),
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration."""
        default_config = {
            "sites": {
                "face_the_nation": {
                    "base_url": "https://www.cbsnews.com/news/face-the-nation-full-transcript-{date}/",
                    "check_interval": 2,
                    "days_to_check": 7,
                    "active_hours": {},
                    "active_days": [],
                    "parser": "cbs_news",
                    "request_delay": 1,
                    "retry_attempts": 5,
                    "timeout": 15
                }
            },
            "parsers": {
                "cbs_news": {
                    "start_markers": ["Full transcript of", "Face the Nation", "MARGARET BRENNAN:"],
                    "end_markers": ["©", "All rights reserved", "This transcript was compiled"],
                    "content_selector": "div.content__body, div.entry__body, article.content",
                    "date_format": "%m-%d-%Y",
                    "date_selector": "time.updated, .date, .timestamp"
                }
            },
            "storage": {
                "output_dir": "./transcripts",
                "format": "txt",
                "include_metadata": True
            },
            "logging": {
                "level": "INFO",
                "max_size_mb": 10,
                "backup_count": 5
            }
        }
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                return self._deep_merge(default_config, config)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Using default config due to: {e}")
            return default_config

    def _deep_merge(self, d1: Dict, d2: Dict) -> Dict:
        """Recursively merge two dictionaries."""
        result = d1.copy()
        for k, v in d2.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def _request_with_retry(self, url: str, method: str = 'GET', **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and delay."""
        site_config = next(iter(self.config.get('sites', {}).values()), {})
        
        # Add delay between requests
        if hasattr(self, '_last_request_time'):
            delay = site_config.get('request_delay', 2)
            elapsed = time.time() - self._last_request_time
            if elapsed < delay:
                time.sleep(delay - elapsed)
        
        try:
            response = self.session.request(
                method,
                url,
                timeout=site_config.get('timeout', 30),
                **kwargs
            )
            response.raise_for_status()
            self._last_request_time = time.time()
            return response
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {url}: {e}")
            return None

    def get_current_est(self) -> datetime:
        """Get current time in EST."""
        return datetime.now(self.EST)

    def should_check_site(self, site_config: Dict) -> bool:
        """Check if we should check the site based on time and day."""
        now = self.get_current_est()
        weekday = now.weekday()
        hour = now.hour
        
        if 'active_days' in site_config and site_config['active_days'] and weekday not in site_config['active_days']:
            self.logger.debug(f"Skipping check - today is {weekday} not in {site_config['active_days']}")
            return False
            
        if 'active_hours' in site_config and site_config['active_hours']:
            start_hour = site_config['active_hours']['start']
            end_hour = site_config['active_hours']['end']
            if not (start_hour <= hour < end_hour):
                self.logger.debug(f"Outside active hours ({start_hour}-{end_hour}), current hour: {hour}")
                return False
                
        return True

    def generate_url(self, site_name: str, date: Optional[datetime] = None) -> Tuple[Optional[str], str]:
        """Generate URL for a given site and date."""
        if date is None:
            date = self.get_current_est()
            
        site_config = self.config['sites'].get(site_name)
        if not site_config:
            self.logger.error(f"No configuration found for site: {site_name}")
            return None, ""
            
        try:
            date_str = date.strftime("%m-%d-%Y")
            url = site_config['base_url'].format(date=date_str)
            self.logger.debug(f"Generated URL: {url}")
            return url, f"{site_name}_{date_str}"
        except KeyError as e:
            self.logger.error(f"Error generating URL: {e}")
            return None, ""

    def generate_multiple_urls(self, site_name: str) -> List[Tuple[str, str]]:
        """Generate multiple URLs for different dates to catch transcripts faster."""
        urls = []
        now = self.get_current_est()
    
        # Check today, yesterday, and past 7 days
        for days_back in range(7):
            check_date = now - timedelta(days=days_back)
            url, date_str = self.generate_url(site_name, check_date)
            if url:
                urls.append((url, f"{site_name}_{date_str}"))
        return urls

    def _extract_content(self, soup: BeautifulSoup, parser_config: Dict) -> str:
        """Extract and clean content from BeautifulSoup object."""
        try:
            # Try content selector first
            if 'content_selector' in parser_config:
                content = soup.select_one(parser_config['content_selector'])
                if content:
                    return self._clean_text(content.get_text())
            
            # Fallback to start/end markers
            text = soup.get_text()
            start_pos = 0
            for marker in parser_config.get('start_markers', []):
                pos = text.lower().find(marker.lower())
                if pos != -1:
                    start_pos = pos
                    break
            
            end_pos = len(text)
            for marker in parser_config.get('end_markers', []):
                pos = text.lower().find(marker.lower(), start_pos)
                if pos != -1:
                    end_pos = pos
                    break
            
            return self._clean_text(text[start_pos:end_pos])
            
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text."""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        return '\n'.join(lines)

    def _save_transcript(self, content: str, metadata: Dict) -> bool:
        """Save transcript to file with metadata."""
        try:
            storage_config = self.config.get('storage', {})
            output_dir = Path(storage_config.get('output_dir', './transcripts'))
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Create filename from date or timestamp
            date_str = metadata.get('date', datetime.now().strftime('%Y%m%d_%H%M%S'))
            output_path = output_dir / f"transcript_{date_str}.{storage_config.get('format', 'txt')}"
            
            # Prepare content
            if storage_config.get('include_metadata', True):
                content = f"URL: {metadata.get('url', '')}\n" \
                         f"Date: {metadata.get('date', '')}\n" \
                         f"Scraped: {metadata.get('scraped_at', '')}\n\n{content}"
            
            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"Saved transcript to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving transcript: {e}")
            return False

    def run(self):
        """Main execution loop."""
        self.logger.info("Starting scraper")
        
        while True:
            try:
                self._check_sites()
                check_interval = next(iter(self.config.get('sites', {}).values()), {}).get('check_interval', 60)
                time.sleep(check_interval)
            except KeyboardInterrupt:
                self.logger.info("Scraper stopped by user")
                break
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                time.sleep(60)  # Prevent tight loop on errors

    def _check_sites(self):
        """Check all configured sites for new content."""
        for site_name, site_config in self.config.get('sites', {}).items():
            print(f"DEBUG: About to check {site_name}")
            print(f"DEBUG: Config: {site_config}")
            print(f"DEBUG: should_check_site: {self.should_check_site(site_config)}")
            
            if not self.should_check_site(site_config):
                print(f"DEBUG: Skipping {site_name}")
                continue
                
            self.logger.info(f"Checking {site_name}...")
            self._process_site(site_name, site_config)

    def _process_site(self, site_name: str, site_config: Dict):
        """Process a single site with multiple date checking."""
        # Generate URLs for multiple dates
        urls_to_check = self.generate_multiple_urls(site_name)
        print(f"DEBUG: Generated {len(urls_to_check)} URLs to check")
        
        for url, date_str in urls_to_check:
            print(f"DEBUG: Checking URL: {url}")
            
            # Check if we already have this transcript
            storage_config = self.config.get('storage', {})
            output_dir = Path(storage_config.get('output_dir', './transcripts'))
            existing_file = output_dir / f"transcript_{date_str}.{storage_config.get('format', 'txt')}"
            
            if existing_file.exists():
                print(f"DEBUG: Already have transcript for {date_str}, skipping")
                continue
            
            # Fetch and parse the page
            response = self._request_with_retry(url)
            if not response:
                print(f"DEBUG: No response for {url}")
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            parser_config = self.config['parsers'].get(site_config['parser'], {})
            
            # Extract content
            content = self._extract_content(soup, parser_config)
            if not content:
                print(f"DEBUG: No content found at {url}")
                continue
                
            # Found content! Save it immediately
            self.logger.info(f"🎉 FOUND TRANSCRIPT at {url}")
            
            # Prepare metadata
            metadata = {
                'url': url,
                'date': date_str,
                'scraped_at': datetime.now().isoformat(),
                'site': site_name
            }
            
            # Save transcript
            if self._save_transcript(content, metadata):
                self.logger.info(f"✅ SUCCESS: Saved transcript from {url}")
                # You could add notification here
            else:
                self.logger.error(f"❌ Failed to save transcript from {url}")

if __name__ == "__main__":
    try:
        scraper = TranscriptScraper()
        scraper.run()
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        print(f"Fatal error: {e}")
        raise
