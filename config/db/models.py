from typing import List

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Event(Base):
    """
    Represents an event in the database.
    """

    __tablename__ = "events"

    id: int = Column(Integer, primary_key=True)
    name: str = Column(String)
    type: str = Column(String)
    event_id: str = Column(String)
    event_url: str = Column(String)
    event_image: str = Column(String)
    event_date: str = Column(String)
    event_time: str = Column(String)
    timezone: str = Column(String)
    segment: str = Column(String)
    genre: str = Column(String)
    sub_genre: str = Column(String)
    currency: str = Column(String)
    price_range_min: str = Column(String)
    price_range_max: str = Column(String)
    age_restriction: str = Column(String)
    venue_name: str = Column(String)
    venue_city: str = Column(String)
    venue_state: str = Column(String)
    venue_country: str = Column(String)
    venue_address: str = Column(String)
    longitude: str = Column(String)
    latitude: str = Column(String)
    dmas: str = Column(String)
    attractions: str = Column(String)


class Attraction(Base):
    """
    Represents an attraction in the database.
    """

    __tablename__ = "attractions"

    name: str = Column(String)
    attraction_id: str = Column(String, primary_key=True)
    attraction_type: str = Column(String)
    attraction_url: str = Column(String)
    attraction_image: str = Column(String)
    segment: str = Column(String)
    genre: str = Column(String)
    sub_genre: str = Column(String)
