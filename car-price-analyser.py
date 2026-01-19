from extractor.IkmanExtractor import IkmanExtractor
from extractor.RiyasewanaExtractor import RiyasewanaExtractor

if __name__ == "__main__":

    car_make_model = {
        "toyota": ["yaris"]
    }

    riyasewanaExtractor = RiyasewanaExtractor()

    for make in car_make_model :
        for model in car_make_model[make]:
            cars = riyasewanaExtractor.extract_data(vehicle_type= "cars", make= make, model= model)
            print(f"Scraping complete. Total cars: {len(cars)}")

            # ikmanExtractor = IkmanExtractor()
            # ikmanExtractor.extract_data(model="vitz")