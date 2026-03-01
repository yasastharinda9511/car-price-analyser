import re
import psycopg2
from psycopg2.extras import execute_values
from dataclasses import fields, asdict, is_dataclass
from typing import List


class DbExporter:

    def __init__(self, host="localhost", port=5432, database="car_analyzer",
                 user="postgres", password="postgres", sslmode="require"):
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
            "sslmode": sslmode
        }
        self._connection = None

    def connect(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self.connection_params)
        return self._connection

    def close(self):
        if self._connection and not self._connection.closed:
            self._connection.close()

    def create_table(self):
        """Create the cars table if it doesn't exist."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS cars (
            id SERIAL PRIMARY KEY,
            title VARCHAR(500),
            make VARCHAR(100),
            model VARCHAR(100),
            yom INTEGER,
            price INTEGER,
            mileage INTEGER,
            location VARCHAR(200),
            gear VARCHAR(50),
            contact VARCHAR(100),
            url VARCHAR(1000) UNIQUE,
            date VARCHAR(50),
            engine VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_cars_make ON cars(make);
        CREATE INDEX IF NOT EXISTS idx_cars_model ON cars(model);
        CREATE INDEX IF NOT EXISTS idx_cars_yom ON cars(yom);
        """
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

    def save_to_db(self, objects: List, skip_duplicates: bool = True):
        """
        Save car objects to the database.

        Args:
            objects: List of Car dataclass instances
            skip_duplicates: If True, skip records with duplicate URLs
        """
        if not objects:
            return 0

        if not is_dataclass(objects[0]):
            raise TypeError("Objects must be dataclass instances")

        field_names = [f.name for f in fields(type(objects[0]))]

        insert_sql = f"""
        INSERT INTO cars ({', '.join(field_names)})
        VALUES %s
        {'ON CONFLICT (url) DO NOTHING' if skip_duplicates else
         'ON CONFLICT (url) DO UPDATE SET ' +
         ', '.join(f'{f} = EXCLUDED.{f}' for f in field_names if f != 'url')}
        """

        values = [self._to_db_row(obj) for obj in objects]

        conn = self.connect()
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
            inserted_count = cur.rowcount
        conn.commit()

        return inserted_count

    def _to_db_row(self, obj) -> tuple:
        """Convert a Car dataclass to a DB-ready tuple, parsing integer fields."""
        d = asdict(obj)

        def parse_int(value: str, min_val: int = 0, max_val: int = 2_147_483_647):
            if not value or not value.strip():
                return None
            digits = re.sub(r'[^\d]', '', value)
            if not digits:
                return None
            num = int(digits)
            return num if min_val <= num <= max_val else None

        d['yom'] = parse_int(d.get('yom', ''), min_val=1900, max_val=2100)
        d['price'] = parse_int(d.get('price', ''))
        d['mileage'] = parse_int(d.get('mileage', ''))

        return tuple(d.values())

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    import csv
    import sys
    import os
    import glob
    from dotenv import load_dotenv

    # Load .env file from project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(project_root, ".env"))

    sys.path.insert(0, project_root)
    from dto.Car import Car

    def load_csv(filepath: str) -> List[Car]:
        """Load cars from a CSV file."""
        cars = []
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cars.append(Car(**row))
        return cars

    def get_csv_files(path: str) -> List[str]:
        """Get all CSV files from a path (file or directory)."""
        if os.path.isfile(path):
            return [path] if path.endswith('.csv') else []
        elif os.path.isdir(path):
            return glob.glob(os.path.join(path, "*.csv"))
        else:
            return []

    if len(sys.argv) < 2:
        print("Usage: python DbExport.py <csv_file_or_directory> [--update]")
        print("  --update: Update existing records instead of skipping duplicates")
        sys.exit(1)

    input_path = sys.argv[1]
    skip_duplicates = "--update" not in sys.argv

    csv_files = get_csv_files(input_path)
    if not csv_files:
        print(f"No CSV files found in: {input_path}")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV file(s)")

    db_config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME", "car_analyzer"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "sslmode": os.getenv("DB_SSLMODE", "require")
    }

    total_loaded = 0
    total_inserted = 0

    with DbExporter(**db_config) as exporter:
        print("Creating table if not exists...")
        exporter.create_table()

        for csv_file in csv_files:
            print(f"\nProcessing: {os.path.basename(csv_file)}")
            cars = load_csv(csv_file)
            print(f"  Loaded {len(cars)} cars")
            total_loaded += len(cars)

            count = exporter.save_to_db(cars, skip_duplicates=skip_duplicates)
            print(f"  Inserted/updated {count} records")
            total_inserted += count

    print(f"\n--- Summary ---")
    print(f"Total files processed: {len(csv_files)}")
    print(f"Total cars loaded: {total_loaded}")
    print(f"Total records inserted/updated: {total_inserted}")
