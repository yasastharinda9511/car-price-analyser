from extractor.IkmanExtractor import IkmanExtractor
from extractor.RiyasewanaExtractor import RiyasewanaExtractor

if __name__ == "__main__":

    riyasewanaExtractor = RiyasewanaExtractor()
    cars = riyasewanaExtractor.extract_data(vehicle_type= "cars", make= "toyota", model="vitz")
    print(f"Scraping complete. Total cars: {len(cars)}")

    # ikmanExtractor = IkmanExtractor()
    # ikmanExtractor.extract_data(model="vitz")