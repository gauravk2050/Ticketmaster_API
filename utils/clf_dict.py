# Description: This module contains functions to read JSON files
# and fetch data from the Ticketmaster API.

import json
import logging
from typing import Dict, Union

import requests

logger = logging.getLogger(__name__)


def read_json_file(file_path: str) -> Union[Dict, None]:
    """
    Reads a JSON file and returns its contents as a dictionary.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict or None: The contents of the JSON file as a dictionary, or None if an error occurs.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data
    except FileNotFoundError as err:
        logger.info("Error: The file %s does not exist.", file_path)
        raise ValueError(f"Error: The file {file_path} does not exist.") from err
    except json.JSONDecodeError as err:
        logger.info("Error: The file %s is not a valid JSON file.", file_path)
        raise ValueError(
            f"Error: The file {file_path} is not a valid JSON file."
        ) from err
    except Exception as err:
        logger.info("An unexpected error occurred: %s", err)
        raise ValueError(f"An unexpected error occurred: {err}") from err


def get_classifications_data(api_key: str) -> Dict:
    """
    Get the classifications data from the Ticketmaster API.

    Args:
        api_key (str): The API key used to fetch data.

    Returns:
        dict: The classifications data.
    """
    url = f"https://app.ticketmaster.com/discovery/v2/classifications.json?apikey={api_key}&size=200"
    response = requests.get(url, timeout=360)

    if response.status_code == 200:
        return response.json()
    else:
        logger.info(
            "Error: Unable to fetch data. Status code: %s", response.status_code
        )
        raise ValueError(
            f"Error: Unable to fetch data. Status code: {response.status_code}"
        )


def process_json_data(api_key: str) -> Dict:
    """
    Processes the input JSON data to create a new structure.

    Args:
        api_key (str): The API key used to fetch data.

    Returns:
        dict: Processed JSON data.
    """

    data = get_classifications_data(api_key)

    if not data:
        logger.info("Error: No data returned from the API.")
        raise ValueError("Error: No data returned from the API.")

    processed_data = {}

    try:
        classifications = data.get("_embedded", {}).get("classifications", [])
        for classification in classifications:
            segment = f"{classification.get('segment', {}).get('name')}-{classification.get('segment', {}).get('id')}"
            if segment:
                processed_data[segment] = {"genres": []}
                genres = (
                    classification.get("segment", {})
                    .get("_embedded", {})
                    .get("genres", [])
                )
                for genre in genres:
                    genre_dict = {
                        "id": genre.get("id"),
                        "name": genre.get("name"),
                        "subgenres": [],
                    }
                    subgenres = genre.get("_embedded", {}).get("subgenres", [])
                    for subgenre in subgenres:
                        subgenre_dict = {
                            "id": subgenre.get("id"),
                            "name": subgenre.get("name"),
                        }
                        genre_dict["subgenres"].append(subgenre_dict)
                    processed_data[segment]["genres"].append(genre_dict)
    except KeyError as e:
        logger.info("Error processing JSON data: Missing key %s", e)
        raise ValueError(f"Error processing JSON data: Missing key {e}") from e
    except Exception as e:
        logger.info("An unexpected error occurred while processing data: %s", e)
        raise ValueError(
            f"An unexpected error occurred while processing data: {e}"
        ) from e

    return processed_data
