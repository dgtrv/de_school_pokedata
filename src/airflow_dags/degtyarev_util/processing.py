"""Module for data cleansing and processing functions."""
from typing import List


def clean_json_data(json_obj: object, keys_to_use: List[str]) -> dict:
    """
    Clean supplied json object and return as a dictionary.

    Remove unnecessary data parts from json object. Resulting dict
    should contain only keys mentioned in keys_to_leave parameter.
    """
    result = {}

    for key in keys_to_use:
        result[key] = json_obj[key]

    return result


def restore_original_pokemon_types(json_obj: dict) -> dict:
    """
    Restore original pokemon types if they changed.

    Analyze past_types key and restore original pokemon types if
    they has been changed. For generations and types analysis.
    """
    past_types = json_obj["past_types"]
    if len(past_types) > 0:
        current_types = json_obj["types"]
        json_obj["types"] = past_types[0]["types"]
        for i in range(1, len(past_types)):
            past_types[i - 1]["types"] = past_types[i]["types"]
        past_types[-1]["types"] = current_types

    return json_obj
