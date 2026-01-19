from extractor.IkmanExtractor import IkmanExtractor
from extractor.RiyasewanaExtractor import RiyasewanaExtractor

if __name__ == "__main__":

    vehicles = {
        "cars": {
            "toyota": ["vitz", "yaris", "passo"],
            "suzuki": ["alto", "wagon-r"]
        },
        "suv": {
            "toyota": ["yaris"]
        }
    }

    riyasewanaExtractor = RiyasewanaExtractor()

    for vehicle_type, makes in vehicles.items():
        for make, models in makes.items():
            for model in models:
                print(f"Scraping {vehicle_type}/{make}/{model}...")
                cars = riyasewanaExtractor.extract_data(
                    vehicle_type=vehicle_type,
                    make=make,
                    model=model
                )
                print(f"Scraping complete. Total: {len(cars)}")

    # ikmanExtractor = IkmanExtractor()
    # ikmanExtractor.extract_data(model="vitz")