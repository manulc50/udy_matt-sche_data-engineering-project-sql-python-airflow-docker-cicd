import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    today = date.today()
    json_path = f"./data/YT_data_{today}.json"

    try:
        logger.info("Processing the file: YT_data_{}.json".format(today))

        with open(json_path, "r", encoding="utf-8") as json_file:
            json_data = json.load(json_file)

        return json_data
    
    except FileNotFoundError:
        logger.error("File not found: {}".format(json_path))
        raise

    except json.JSONDecodeError:
        logger.error("Invalid JSON format in file: {}".format(json_path))
        raise