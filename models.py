import os
import json
from dotenv import load_dotenv

import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker, declarative_base, relationship


load_dotenv()
DB_USER = os.getenv("USER")
DB_PASSWORD = os.getenv("PASSWORD")

DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/netology_db"
engine = sq.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()
class Publisher(Base):
    __tablename__ = "publishers"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String (45), nullable=False, unique=True)

class Book(Base):
    __tablename__ = "books"
    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String (50), unique=True, nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publishers.id"), nullable=False)

    publishers = relationship(Publisher, backref='books')

class Shop(Base):
    __tablename__ = "shops"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String (50), unique=True, nullable=False)

class Stock(Base):
    __tablename__ = "stocks"
    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("books.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shops.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref='stocks')
    shop = relationship(Shop, backref='stocks')

class Sale(Base):
    __tablename__ = "sales"
    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stocks.id"), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref='sales')

def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def data_recording(path):
    with open(path, 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publishers': Publisher,
            'shops': Shop,
            'books': Book,
            'stocks': Stock,
            'sales': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()

def get_shops(author_name_or_id):
    query = session.query(
        Book,
        Shop,
        Sale.price,
        Sale.date_sale
    ).select_from(Shop).\
    join(Stock).\
    join(Book).\
    join(Publisher).\
    join(Sale)
    
    if author_name_or_id.isdigit():
        query = query.filter(Publisher.id == int(author_name_or_id))
    else:
        query = query.filter(Publisher.name == author_name_or_id)

    for book, shop, price, date_sale in query.all():
        print(f"{book.title: <40} | {shop.name: <10} | {price: <8} | {date_sale.strftime('%d-%m-%Y')}")
    
session.close()