# Description: Helper functions for processing data 
# and interacting with the database.

import logging
from typing import Dict, List

import pandas as pd
import requests
from sqlalchemy.engine import Engine
from sqlalchemy.inspection import inspect

from config.db.models import Attraction, Event

logger = logging.getLogger(__name__)

def process_data(
    db_df: pd.DataFrame, engine: Engine, file_path: str, subset_: str, table_name: str
) -> None:
    """
    Process the data and perform operations based on the given DataFrame.

    Args:
        db_df (pd.DataFrame): The DataFrame containing data from the database.
        engine (Engine): The SQLAlchemy engine object.
        file_path (str): The path to the CSV file.
        subset_ (str): The column name to use for identifying duplicates.
        table_name (str): The name of the table in the database.

    Returns:
        None
    """
    if db_df.empty:
        logger.info("No data in the database.")
        final_df = pd.read_csv(file_path)
    else:
        # read the csv file
        csv_df = pd.read_csv(file_path)

        merged_df = pd.merge(csv_df, db_df, how="left", indicator=True)

        # Filter rows from csv_df that are not in db_df
        not_in_db_df = merged_df[merged_df["_merge"] == "left_only"]

        # Drop the indicator column if you don't need it
        final_df = not_in_db_df.drop(columns=["_merge"])

    logger.info(final_df.count())

    duplicated_rows = final_df[final_df.duplicated(subset=subset_)]
    logger.info(f"Number of duplicated rows: {duplicated_rows.shape[0]}")

    df_unique = final_df.drop_duplicates(subset=[subset_])

    # write to the database
    df_unique.to_sql(table_name, con=engine, if_exists="append", index=False)


def check_tables_exist(engine_: Engine) -> bool:
    """
    Check if the specified tables exist in the database.

    Args:
        engine_ (Engine): The SQLAlchemy engine object.

    Returns:
        bool: True if all tables exist, False otherwise.
    """
    inspector = inspect(engine_)
    return all(
        inspector.has_table(table_name)
        for table_name in ["events", "attractions"]
    )

def get_all_attractions(api_key: str, info) -> None:
    """
    Fetches all attractions from the Ticketmaster API and saves them to the database.

    Args:
        api_key (str): The API key for accessing the Ticketmaster API.
    """
    segment_json = info

    # open a csv file
    with open("./data/raw_data/attraction.csv", "w", encoding="utf-8") as file:
        file.write(
            "name,attraction_id,attraction_type,attraction_url,attraction_image,segment,genre,sub_genre\n"
        )
        for segment_info in segment_json:
            for genres in segment_json[segment_info]["genres"]:
                for sub_genre in genres["subgenres"]:

                    segment_id = segment_info.split("-")[-1]
                    genres_id = genres.get("id")
                    sub_genre_id = sub_genre.get("id")
                    page = 0

                    while True:
                        url = f"https://app.ticketmaster.com/discovery/v2/attractions.json?apikey={api_key}&size=200&page={page}&segmentId={segment_id}&genreId={genres_id}&subGenreId={sub_genre_id}"
                        response = requests.get(url, timeout=360)
                        data = response.json()
                        if "_embedded" not in data:
                            break
                        count = 0
                        for attraction in data["_embedded"]["attractions"]:
                            name = attraction.get("name")
                            attraction_id = attraction.get("id")
                            attraction_type = attraction.get("type")
                            attraction_url = attraction.get("url")
                            attraction_image = attraction.get("images", [{}])[0].get(
                                "url"
                            )
                            segment = (
                                attraction.get("classifications", [{}])[0]
                                .get("segment", {})
                                .get("name")
                            )
                            genre = (
                                attraction.get("classifications", [{}])[0]
                                .get("genre", {})
                                .get("name")
                            )
                            sub_genre = (
                                attraction.get("classifications", [{}])[0]
                                .get("subGenre", {})
                                .get("name")
                            )

                            # Replace comma with /
                            if name is not None and "," in name:
                                name = name.replace(",", "/")
                            if attraction_id is not None and "," in attraction_id:
                                attraction_id = attraction_id.replace(",", "/")
                            if attraction_type is not None and "," in attraction_type:
                                attraction_type = attraction_type.replace(",", "/")
                            if attraction_url is not None and "," in attraction_url:
                                attraction_url = attraction_url.replace(",", "/")
                            if attraction_image is not None and "," in attraction_image:
                                attraction_image = attraction_image.replace(",", "/")
                            if segment is not None and "," in segment:
                                segment = segment.replace(",", "/")
                            if genre is not None and "," in genre:
                                genre = genre.replace(",", "/")
                            if sub_genre is not None and "," in sub_genre:
                                sub_genre = sub_genre.replace(",", "/")

                            # write to the csv file
                            file.write(
                                f"{name},{attraction_id},{attraction_type},{attraction_url},{attraction_image},{segment},{genre},{sub_genre}\n"
                            )

                            count += 1

                        if data["page"]["totalPages"] == page - 1:
                            break

                        logger.info(f"Page {page} done. and {count} attractions added.")
                        page += 1


def get_all_events(
    api_key: str, segment_json: Dict[str, Dict[str, List[Dict[str, str]]]]
) -> None:
    """
    Fetches all events from the Ticketmaster API and saves them to the database.

    Args:
        api_key (str): The API key for accessing the Ticketmaster API.
        segment_json (Dict[str, Dict[str, List[Dict[str, str]]]]): A dictionary containing segment information.

    Returns:
        None
    """
    page = 0

    # open a csv file
    with open("./data/raw_data/events.csv", "w", encoding="utf-8") as file:
        file.write(
            "name,type,event_id,event_url,event_image,event_date,event_time,timezone,segment,genre,sub_genre,currency,price_range_min,price_range_max,age_restriction,venue_name,venue_city,venue_state,venue_country,venue_address,longitude,latitude,dmas,attractions\n"
        )

        end_date = "2024-06-10T23:59:59Z"

        for segment_info in segment_json:
            for genres in segment_json[segment_info]["genres"]:
                for sub_genre in genres["subgenres"]:

                    segment_id = segment_info.split("-")[-1]
                    genres_id = genres.get("id")
                    sub_genre_id = sub_genre.get("id")
                    page = 0

                    while True:
                        url = f"https://app.ticketmaster.com/discovery/v2/events.json?apikey={api_key}&size=200&page={page}&segmentId={segment_id}&genreId={genres_id}&subGenreId={sub_genre_id}&endDateTime={end_date}"
                        response = requests.get(url, timeout=360)
                        data = response.json()
                        if "_embedded" not in data:
                            break
                        count = 0
                        for event in data["_embedded"]["events"]:
                            name = event.get("name")
                            type = event.get("type")
                            event_id = event.get("id")
                            event_url = event.get("url")
                            event_image = event.get("images", [{}])[0].get("url")
                            event_date = (
                                event.get("dates", {}).get("start", {}).get("localDate")
                            )
                            event_time = (
                                event.get("dates", {}).get("start", {}).get("localTime")
                            )
                            timezone = event.get("dates", {}).get("timezone")
                            segment = (
                                event.get("classifications", [{}])[0]
                                .get("segment", {})
                                .get("name")
                            )
                            genre = (
                                event.get("classifications", [{}])[0]
                                .get("genre", {})
                                .get("name")
                            )
                            sub_genre = (
                                event.get("classifications", [{}])[0]
                                .get("subGenre", {})
                                .get("name")
                            )
                            currency = event.get("priceRanges", [{}])[0].get("currency")
                            price_range_min = event.get("priceRanges", [{}])[0].get(
                                "min"
                            )
                            price_range_max = event.get("priceRanges", [{}])[0].get(
                                "max"
                            )
                            age_restriction = str(event.get("ageRestrictions"))
                            venue_name = (
                                event["_embedded"].get("venues", [{}])[0].get("name")
                            )
                            venue_city = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("city", {})
                                .get("name")
                            )
                            venue_state = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("state", {})
                                .get("name")
                            )
                            venue_country = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("country", {})
                                .get("countryCode")
                            )
                            venue_address = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("address", {})
                                .get("line1")
                            )
                            longitude = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("location", {})
                                .get("longitude")
                            )
                            latitude = (
                                event["_embedded"]
                                .get("venues", [{}])[0]
                                .get("location", {})
                                .get("latitude")
                            )
                            dmas = ", ".join(
                                [
                                    str(dma.get("id"))
                                    for dma in event["_embedded"]
                                    .get("venues", [{}])[0]
                                    .get("dmas", [])
                                ]
                            )
                            attractions = ", ".join(
                                [
                                    attraction.get("id")
                                    for attraction in event["_embedded"].get(
                                        "attractions", []
                                    )
                                ]
                            )

                            # Replace comma with /
                            if name is not None and "," in name:
                                name = name.replace(",", "/")
                            if type is not None and "," in type:
                                type = type.replace(",", "/")
                            if event_id is not None and "," in event_id:
                                event_id = event_id.replace(",", "/")
                            if event_url is not None and "," in event_url:
                                event_url = event_url.replace(",", "/")
                            if event_image is not None and "," in event_image:
                                event_image = event_image.replace(",", "/")
                            if event_date is not None and "," in event_date:
                                event_date = event_date.replace(",", "/")
                            if event_time is not None and "," in event_time:
                                event_time = event_time.replace(",", "/")
                            if timezone is not None and "," in timezone:
                                timezone = timezone.replace(",", "/")
                            if segment is not None and "," in segment:
                                segment = segment.replace(",", "/")
                            if genre is not None and "," in genre:
                                genre = genre.replace(",", "/")
                            if sub_genre is not None and "," in sub_genre:
                                sub_genre = sub_genre.replace(",", "/")
                            if currency is not None and "," in currency:
                                currency = currency.replace(",", "/")
                            if age_restriction is not None and "," in age_restriction:
                                age_restriction = age_restriction.replace(",", "/")
                            if venue_name is not None and "," in venue_name:
                                venue_name = venue_name.replace(",", "/")
                            if venue_city is not None and "," in venue_city:
                                venue_city = venue_city.replace(",", "/")
                            if venue_state is not None and "," in venue_state:
                                venue_state = venue_state.replace(",", "/")
                            if venue_country is not None and "," in venue_country:
                                venue_country = venue_country.replace(",", "/")
                            if venue_address is not None and "," in venue_address:
                                venue_address = venue_address.replace(",", "/")
                            if longitude is not None and "," in longitude:
                                longitude = longitude.replace(",", "/")
                            if latitude is not None and "," in latitude:
                                latitude = latitude.replace(",", "/")
                            if dmas is not None and "," in dmas:
                                dmas = dmas.replace(",", "/")
                            if attractions is not None and "," in attractions:
                                attractions = attractions.replace(",", "/")

                            # write to the csv file
                            file.write(
                                f"{name},{type},{event_id},{event_url},{event_image},{event_date},{event_time},{timezone},{segment},{genre},{sub_genre},{currency},{price_range_min},{price_range_max},{age_restriction},{venue_name},{venue_city},{venue_state},{venue_country},{venue_address},{longitude},{latitude},{dmas},{attractions}\n"
                            )

                            count += 1

                        if data["page"]["totalPages"] == page - 1:
                            break

                        logger.info(f"Page {page} done. and {count} events added.")
                        page += 1


def get_all_attractions_from_db(Session) -> pd.DataFrame:
    """
    Retrieve all attractions from the database and return them as a pandas DataFrame.

    Parameters:
    - Session: SQLAlchemy session object

    Returns:
    - df: pandas DataFrame containing the attraction data
    """
    session = Session()
    attractions = session.query(Attraction).all()

    df = pd.DataFrame(
        [
            {
                "name": attraction.name,
                "attraction_id": attraction.attraction_id,
                "attraction_type": attraction.attraction_type,
                "attraction_url": attraction.attraction_url,
                "attraction_image": attraction.attraction_image,
                "segment": attraction.segment,
                "genre": attraction.genre,
                "sub_genre": attraction.sub_genre,
            }
            for attraction in attractions
        ]
    )

    session.close()
    return df

def get_all_events_from_db(Session) -> pd.DataFrame:
    """
    Retrieve all events from the database and return them as a pandas DataFrame.

    Parameters:
    - Session: SQLAlchemy session object

    Returns:
    - df: pandas DataFrame containing the event data

    """
    session = Session()
    events = session.query(Event).all()

    df = pd.DataFrame(
        [
            {
                "name": event.name,
                "type": event.type,
                "event_id": event.event_id,
                "event_url": event.event_url,
                "event_image": event.event_image,
                "event_date": event.event_date,
                "event_time": event.event_time,
                "timezone": event.timezone,
                "segment": event.segment,
                "genre": event.genre,
                "sub_genre": event.sub_genre,
                "currency": event.currency,
                "price_range_min": event.price_range_min,
                "price_range_max": event.price_range_max,
                "age_restriction": event.age_restriction,
                "venue_name": event.venue_name,
                "venue_city": event.venue_city,
                "venue_state": event.venue_state,
                "venue_country": event.venue_country,
                "venue_address": event.venue_address,
                "longitude": event.longitude,
                "latitude": event.latitude,
                "dmas": event.dmas,
                "attractions": event.attractions,
            }
            for event in events
        ]
    )

    session.close()
    return df
