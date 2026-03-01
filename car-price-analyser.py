from extractor.IkmanExtractor import IkmanExtractor
from extractor.RiyasewanaExtractor import RiyasewanaExtractor

if __name__ == "__main__":

    # Popular vehicles in Sri Lanka
    vehicles = {
        "cars": {
            "toyota": [
                "vitz", "yaris", "corolla", "axio", "premio", "allion",
                "prius", "aqua", "passo", "belta", "roomy"
            ],
            "suzuki": ["alto", "wagon-r", "swift", "celerio", "ciaz", "every", "baleno"],
            "honda": ["fit", "grace", "civic", "city", "insight"],
            "nissan": ["march", "note", "leaf", "sunny", "bluebird", "latio"],
            "mazda": ["demio", "axela", "familia"],
            "mitsubishi": ["lancer", "attrage", "mirage"],
            "daihatsu": ["mira", "move", "tanto", "hijet"],
            "perodua": ["axia", "bezza", "viva"],
            "hyundai": ["accent", "elantra", "i10", "i20"],
            "kia": ["rio", "cerato", "picanto", "sorento"]
        },
        "suvs": {
            "toyota": ["raize", "rav4", "land-cruiser", "prado", "fortuner", "rush", "chr"],
            "honda": ["vezel", "cr-v", "hr-v"],
            "nissan": ["x-trail", "juke", "qashqai", "magnite"],
            "mitsubishi": ["outlander", "pajero", "montero", "asx"],
            "suzuki": ["vitara", "jimny", "escudo", "fronx"],
            "mazda": ["cx-3", "cx-5"],
            "hyundai": ["tucson", "santa-fe", "creta"],
            "kia": ["sportage", "sorento", "seltos"]
        },
        "vans": {
            "toyota": ["hiace", "noah", "voxy", "townace"],
            "nissan": ["caravan", "nv200", "serena"],
            "suzuki": ["every"],
            "honda": ["stepwgn"]
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