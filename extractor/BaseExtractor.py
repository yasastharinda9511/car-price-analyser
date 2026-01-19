import csv
import os
import random
import time

from dto.Car import Car


class BaseExtractor:
    def load_existing_from_csv(self, filename) -> list[Car]:
        """Load existing cars from CSV and populate seen_urls set."""
        existing_cars = []
        if not os.path.exists(filename):
            return existing_cars

        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                car = Car(
                    title=row.get("title"),
                    make=row.get("make"),
                    model=row.get("model"),
                    yom=row.get("yom"),
                    price=row.get("price"),
                    mileage=row.get("mileage"),
                    location=row.get("location"),
                    gear=row.get("gear"),
                    contact=row.get("contact"),
                    url=row.get("url"),
                    date=row.get("date")
                )
                existing_cars.append(car)
                if car.url:
                    self.seen_urls.add(car.url)

        print(f"Loaded {len(existing_cars)} existing records from {filename}")
        return existing_cars

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