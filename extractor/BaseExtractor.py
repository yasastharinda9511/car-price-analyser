import random
from datetime import time


class BaseExtractor:
    def fetch_with_retry(self, scraper, url, headers=None, max_retries=5):
        """Fetch URL with exponential backoff on rate limit."""
        for attempt in range(max_retries):
            resp = scraper.get(url, headers=headers)
            if resp.status_code == 429:  # Rate limited
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Rate limited. Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            return resp
        raise Exception(f"Failed to fetch {url} after {max_retries} retries")