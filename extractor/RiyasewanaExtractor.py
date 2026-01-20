import csv
import os
import time
import random

import cloudscraper
from bs4 import BeautifulSoup

from dto.Car import Car
from exporter.CsvExport import CsvExporter
from extractor.BaseExtractor import BaseExtractor

class RiyasewanaExtractor(BaseExtractor):
    def __init__(self):
        self.base_url = "https://riyasewana.com/search"
        self.cars = []
        self.seen_urls = set()  # Track seen URLs to avoid duplicates

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

    def extract_details(self, soup: BeautifulSoup):
        data = {}

        # table layout
        for p in soup.select("p.moreh"):
            td = p.parent.find_next_sibling("td")
            if td:
                data[p.get_text(strip=True)] = td.get_text(strip=True)

        # card layout
        for row in soup.select("div.card-row"):
            label = row.select_one("p.moreh")
            if label:
                key = label.get_text(strip=True)
                value = row.get_text(" ", strip=True).replace(key, "", 1).strip()
                data[key] = value

        # contact
        contact = soup.select_one(".moreph a")
        if contact:
            data["Contact"] = contact.get_text(strip=True)

        return data

    def get_next_page(self, soup) -> str | None:
        """Find the 'Next' link in pagination and return its URL, or None if not found."""
        pagination = soup.select_one("div.pagination")
        if pagination:
            next_link = pagination.find("a", string="Next")
            if next_link and next_link.get("href"):
                href = next_link["href"]
                if href.startswith("//"):
                    href = "https:" + href
                return href
        return None

    def extract_data(self, vehicle_type, make, model) -> list[Car]:
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

        current_url = f"{self.base_url}/{vehicle_type}/{make}/{model}"
        filename = f"{model}-riyasewana.csv"
        page_num = 1
        duplicates_skipped = 0
        self.seen_urls.clear()  # Reset for new extraction

        # Load existing records from CSV to avoid duplicates
        existing_cars = self.load_existing_from_csv(filename)
        cars = []  # New cars only

        while current_url:
            print(f"Fetching page {page_num}: {current_url}")

            resp = self.fetch_with_retry(scraper, current_url, headers=headers)
            soup = BeautifulSoup(resp.text, "html.parser")

            listings = soup.find_all("li", class_="item round")
            listings += [li for li in soup.find_all("li") if li.find("div", class_="item")]

            for item in listings:
                title_tag = item.select_one("h2.more a")
                if not title_tag:
                    continue

                title = title_tag.get_text(strip=True)
                href = title_tag["href"]

                # Skip if already processed (duplicate)
                if href in self.seen_urls:
                    print(f"Skipping duplicate: {href}")
                    duplicates_skipped += 1
                    if duplicates_skipped > 25:
                        break
                    continue
                self.seen_urls.add(href)

                date = item.select_one("div.boxintxt.s").get_text(strip=True)

                print("Title:", title)
                print("Link:", href)

                # Add delay between individual car requests
                time.sleep(random.uniform(1, 3))

                car_node = self.fetch_with_retry(scraper, href, headers=headers)
                car_soup = BeautifulSoup(car_node.text, "html.parser")

                data = self.extract_details(car_soup)

                # Print required fields
                print("Make:", data.get("Make"))
                print("Model:", data.get("Model"))
                print("YOM:", data.get("YOM"))
                print("Contact:", data.get("Contact"))
                print("Price :", data.get("Price"))

                print("########################################")

                car = Car(
                    title=title,
                    make=data.get("Make"),
                    model=data.get("Model"),
                    yom=data.get("YOM"),
                    price=data.get("Price"),
                    mileage=data.get("Mileage (km)"),
                    location=data.get("Location"),
                    gear=data.get("Gear"),
                    contact=data.get("Contact"),
                    url=href,
                    date=date
                )

                cars.append(car)

            # Check for next page
            current_url = self.get_next_page(soup)
            if duplicates_skipped > 25 :
                break
            if current_url:
                page_num += 1
                # Add delay before fetching next page
                time.sleep(random.uniform(2, 4))

        # Merge existing and new cars, then save
        all_cars = existing_cars + cars
        CsvExporter.save_to_csv(objects=all_cars, filename=filename)
        print(f"Extraction complete. New cars: {len(cars)}, Existing: {len(existing_cars)}, Total: {len(all_cars)}, Duplicates skipped: {duplicates_skipped}")
        return all_cars
