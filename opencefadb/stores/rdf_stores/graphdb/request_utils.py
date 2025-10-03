import logging

import requests

logger = logging.getLogger("opencefadb")


def _get_request(*args, **kwargs):
    try:
        return requests.get(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise e


def _post_request(*args, **kwargs):
    try:
        return requests.post(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise e
