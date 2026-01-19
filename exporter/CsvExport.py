import csv
from dataclasses import fields, asdict, is_dataclass


class CsvExporter:

    @staticmethod
    def save_to_csv(objects, filename="data.csv"):
        if not objects:
            return

        cls = type(objects[0])

        if not is_dataclass(objects[0]):
            raise TypeError("Objects must be dataclass instances")

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[f.name for f in fields(cls)]
            )
            writer.writeheader()
            for obj in objects:
                writer.writerow(asdict(obj))