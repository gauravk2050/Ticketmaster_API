import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from config.db.models import Base
from config.environments import load_environment_variables
from utils.clf_dict import process_json_data
from utils.helpers import (
    process_data,
    check_tables_exist,
    get_all_attractions,
    get_all_attractions_from_db,
    get_all_events,
    get_all_events_from_db,
)

logger = logging.getLogger(__name__)


def create_tables(engine_: Engine) -> None:
    """
    Create tables in the database if they do not already exist.

    Args:
        engine (Engine): The SQLAlchemy engine object.

    Returns:
        None
    """
    if not check_tables_exist(engine_):
        Base.metadata.create_all(engine_)
        logger.info("Tables created successfully.")
    else:
        logger.info("Tables already exist.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting the process...")
    logger.info("Loading environment variables...")
    API_KEY = load_environment_variables()
    logger.info("Environment variables loaded successfully.")

    # Create the engine and session
    logger.info("Creating the engine and session...")
    engine = create_engine("sqlite:///database.db")
    Session = sessionmaker(bind=engine)

    # Create tables if they do not exist
    logger.info("Creating tables if they do not exist...")
    create_tables(engine)

    # Get today's date
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    # Get the date 14 days from now
    next_date = datetime.datetime.now() + datetime.timedelta(days=14)
    next_date = next_date.strftime("%Y-%m-%d") + "T23:59:59Z"

    logger.info("Processing classificaions data...")
    info = process_json_data(API_KEY)
    logger.info("Data processed successfully.")

    logger.info("Getting all attractions and events...")
    # Get all attractions available
    get_all_attractions(API_KEY, info)
    logger.info("All attractions retrieved successfully.")

    logger.info("Getting all events...")
    # Get all events available
    get_all_events(API_KEY, info)
    logger.info("All events retrieved successfully.")

    logger.info("Processing attraction data...")
    db_df = get_all_attractions_from_db(Session)
    process_data(
        db_df, engine, "./data/raw_data/attraction.csv", "attraction_id", "attractions"
    )

    logger.info("Processing event data...")
    db_df = get_all_events_from_db(Session)
    process_data(db_df, engine, "./data/raw_data/event.csv", "event_id", "events")
