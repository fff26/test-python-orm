import os
from dotenv import load_dotenv

import sqlalchemy as sq

from models import create_tables, data_recording, get_shops


load_dotenv()
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")
engine = sq.create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/netology_db")

if __name__ == "__main__":

    create_tables(engine)

    path = 'tests_data.json'
    data_recording(path)

    autor = input("Введите автора или его ID:\n")
    get_shops(autor)