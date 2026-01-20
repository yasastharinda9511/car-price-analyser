import re
import time
import random
from datetime import datetime

import cloudscraper
from bs4 import BeautifulSoup

from dto.Car import Car
from exporter.CsvExport import CsvExporter
from extractor.BaseExtractor import BaseExtractor
class IkmanExtractor(BaseExtractor):
    def __init__(self):
        self.base_url = "https://ikman.lk"
        self.seen_urls = set()  # Track seen URLs to avoid duplicates

    def normalize_date(self, raw_date):
        if not raw_date:
            return None

        date_part = " ".join(raw_date.split(" ")[:2])  # "17 Jan"
        now = datetime.now()
        guessed_date = datetime.strptime(f"{date_part} {now.year}", "%d %b %Y")

        # If the date is in the future → it must be last year
        if guessed_date > now:
            guessed_date = guessed_date.replace(year=now.year - 1)

        return guessed_date.strftime("%Y-%m-%d")

    def extract_data(self, model):
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

        filename = f"{model}-ikman.csv"
        page_num = 1
        total_pages = 1
        items_per_page = 25
        duplicates_skipped = 0
        self.seen_urls.clear()  # Reset for new extraction

        # Load existing records from CSV to avoid duplicates
        existing_cars = self.load_existing_from_csv(filename)
        cars = []  # New cars only

        while page_num <= total_pages:

            current_url = f"{self.base_url}/en/ads/sri-lanka/cars?sort=relevance&buy_now=0&urgent=0&query={model}&page={page_num}"
            print(f"Fetching page {page_num}: {current_url}")

            resp = self.fetch_with_retry(scraper, current_url, headers=headers)
            soup = BeautifulSoup(resp.text, "html.parser")

            listings = soup.find_all("li", class_="normal--2QYVk gtm-normal-ad")
            listings += soup.find_all("li", class_ = "top-ads-container--1Jeoq gtm-top-ad")

            span = soup.select_one("span.ads-count-text--1UYy_")

            total_ads = None
            if span:
                text = span.get_text(strip=True)
                match = re.search(r"of\s+(\d+)", text)
                if match:
                    total_ads = int(match.group(1))
                    total_pages = int(total_ads / items_per_page)

            print(total_pages)

            for item in listings:
                link_tag = item.find("a", href=True)

                if not link_tag:
                    continue

                href = link_tag["href"]
                if href.startswith("/"):
                    ad_url = self.base_url + href
                else:
                    ad_url = href

                # Skip if already processed (duplicate)
                if ad_url in self.seen_urls:
                    print(f"Skipping duplicate: {ad_url}")
                    duplicates_skipped += 1
                    if duplicates_skipped > 25:
                        break
                    continue
                duplicates_skipped = 0
                self.seen_urls.add(ad_url)

                # Add delay between individual car requests
                print(ad_url)
                time.sleep(random.uniform(1, 3))

                car_node = self.fetch_with_retry(scraper, ad_url, headers=headers)
                car_soup = BeautifulSoup(car_node.text, "html.parser")

                meta_section = car_soup.find("div", class_="ad-meta--17Bqm")

                car_details = {}

                subtitle = car_soup.select_one("span.sub-title--37mkY")

                posted_on = None
                location = None

                if subtitle:
                    # Date
                    text = subtitle.get_text(" ", strip=True)
                    if "Posted on" in text:
                        posted_on = text.split("Posted on")[1].split(",")[0].strip()

                    # Location
                    locations = [
                        span.text.strip()
                        for span in subtitle.select("a.subtitle-location-link--1q5zA span")
                    ]
                    location = ", ".join(locations)

                print("Posted on:", posted_on)
                print("Location:", location)

                car_details["Date"] = self.normalize_date(posted_on)
                car_details["Location"] = location

                price = meta_section.select_one("div.amount--3NTpl")
                car_details["Price"] = price.text.strip() if price else None

                rows = meta_section.find_all("div", class_="full-width--XovDn")

                for row in rows:
                    label = row.find("div", class_="label--3oVZK")
                    value = row.find("div", class_="value--1lKHt")

                    if not label or not value:
                        continue

                    key = label.get_text(strip=True).replace(":", "")
                    val = value.get_text(" ", strip=True)

                    car_details[key] = val

                title_el = car_soup.select_one("h1.title--3s1R8")

                title = title_el.get_text(strip=True) if title_el else None
                print(title)
                car_details["Title"] = title

                print(car_details)
                if duplicates_skipped > 25:
                    break
                car = Car(
                    title=title,
                    make=car_details.get("Make"),
                    model=car_details.get("Model"),
                    yom=car_details.get("Year of Manufacture"),
                    price=car_details.get("Price"),
                    mileage=car_details.get("Mileage"),
                    location=car_details.get("Location"),
                    gear=car_details.get("Transmission"),
                    contact=car_details.get("Contact"),
                    url=ad_url,
                    date=car_details.get("Date")
                )

                cars.append(car)
            page_num += 1

        # Merge existing and new cars, then save
        all_cars = existing_cars + cars
        CsvExporter.save_to_csv(all_cars, filename=filename)
        print(f"Extraction complete. New cars: {len(cars)}, Existing: {len(existing_cars)}, Total: {len(all_cars)}, Duplicates skipped: {duplicates_skipped}")
        return all_cars