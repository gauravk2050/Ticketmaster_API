# This file contains the test cases for the utils module.
from typing import Any, Dict

import pytest

from utils.clf_dict import process_json_data


def test_process_json_data_with_valid_api_key(mocker) -> None:
    """
    Test case for process_json_data function with a valid API key.

    Args:
        mocker: The mocker object for mocking dependencies.

    Returns:
        None

    Raises:
        AssertionError: If the processed data is not as expected.
    """
    # Mocking the get_classifications_data function to return a sample valid data
    mocked_get_data = mocker.patch("utils.clf_dict.get_classifications_data")
    mocked_get_data.return_value = {
        "_embedded": {
            "classifications": [
                {
                    "segment": {
                        "name": "Segment1",
                        "id": 1,
                        "_embedded": {
                            "genres": [
                                {
                                    "id": 1,
                                    "name": "Genre1",
                                    "_embedded": {
                                        "subgenres": [{"id": 1, "name": "Subgenre1"}]
                                    },
                                }
                            ]
                        },
                    },
                }
            ]
        }
    }

    # Calling the function with a valid API key
    processed_data: Dict[str, Any] = process_json_data("valid_api_key")

    # Asserting that the processed data is as expected
    assert processed_data == {
        "Segment1-1": {
            "genres": [
                {
                    "id": 1,
                    "name": "Genre1",
                    "subgenres": [{"id": 1, "name": "Subgenre1"}],
                }
            ]
        }
    }


def test_process_json_data_with_no_data_return(mocker):
    """
    Test case for process_json_data function with no data returned from the API.

    Args:
        mocker: The mocker object for mocking dependencies.

    Returns:
        None

    Raises:
        ValueError: If no data is returned from the API.
    """
    # Mocking the get_classifications_data function to return None
    mocked_get_data = mocker.patch("utils.clf_dict.get_classifications_data")
    mocked_get_data.return_value = None

    # Calling the function with an invalid API key should raise a ValueError
    with pytest.raises(ValueError):
        process_json_data("api_key")
