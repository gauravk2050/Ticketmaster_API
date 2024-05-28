from typing import Optional, Tuple

from environs import Env


def load_environment_variables() -> Tuple[str, Optional[str], Optional[str]]:
    """
    Load environment variables from a .env file.

    Returns:
        A tuple containing the values of API_KEY, OTHER_VAR1, and OTHER_VAR2.
    Raises:
        ValueError: If API_KEY environment variable is not found.
    """
    env = Env()
    env.read_env()

    # Load API_KEY environment variable
    api_key = env("API_KEY", default=None)
    if api_key is None:
        raise ValueError("API_KEY environment variable not found")

    return api_key
