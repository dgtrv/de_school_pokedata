"""
Main control module containing functions for Airflow DAGs.

Here are the functions used in Airflow DAGs to control API calls,
data transfer to S3, log regular checks. Settings for S3 folders also
and API endpoints are also defined here.
"""
import asyncio
from typing import Callable, List

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from degtyarev_util.poke_api import get_pokeapi_endpoint, make_api_requests
from degtyarev_util.processing import (clean_json_data,
                                       restore_original_pokemon_types)
from degtyarev_util.s3_util import list_s3_keys, save_data_to_s3

PERSONAL_S3_FOLDER_NAME = "Degtyarev"
SNOWPIPE_URI = Variable.get("snowpipe_files")
S3_TARGET_FOLDER = f"{SNOWPIPE_URI}{PERSONAL_S3_FOLDER_NAME}/"
POKE_API_URL = "https://pokeapi.co/api/v2/"
LOG_S3_FOLDER_PATH = f"{S3_TARGET_FOLDER}generations_check_log/"


def _extract_from_api_save_to_s3(
    endpoint: str,
    target_folder: str,
    filename_source_key: str,
    keys_to_use: List[str],
    processing: List[Callable] = None,
) -> None:
    """
    Extract data from PokeAPI and send it to S3 bucket.

    Extract data from API endpoint, clean it (get rid of all the json
    keys except mentioned in keys_to_use), optionally process cleaned
    data with supplied processing functions, send data to S3.
    S3 folder, filename is generated based on the filename_source_key
    with ".json" postfix.

    :param endpoint: API endpoint
    :param target_folder: S3 folder to store output files
    :param filename_source_key: json data key to be used for filename
    generation - preferrably 'id' key
    :param keys_to_use: parts of input data to leave in output file
    :param processing: list of functions to be applied after data
    cleaning
    """
    s3_prefix = f"{S3_TARGET_FOLDER}{target_folder}/"
    api_endpoint = f"{POKE_API_URL}{endpoint}/"

    processing_functions = [
        lambda x: clean_json_data(x, keys_to_use=keys_to_use)
    ]

    if processing:
        processing_functions.extend(processing)

    asyncio.run(
        get_pokeapi_endpoint(
            endpoint_url=api_endpoint,
            save_results_with=lambda x: save_data_to_s3(
                x, s3_prefix, filename_source_key=filename_source_key
            ),
            processing=processing_functions,
        )
    )


def _log(message: str, log_file: str):
    s3hook = S3Hook()
    key = log_file
    s3hook.load_string(string_data=message, key=key)


def display(text: str) -> None:
    """Display supplied text."""
    print(text)


def extract_and_save_data() -> None:
    """Extract data from PokeAPI, clean and process it than send to S3."""
    _extract_from_api_save_to_s3(
        endpoint="pokemon",
        target_folder="pokemon",
        filename_source_key="id",
        keys_to_use=["id", "name", "stats", "past_types", "types"],
        processing=[lambda x: restore_original_pokemon_types(x)],
    )

    _extract_from_api_save_to_s3(
        endpoint="type",
        target_folder="types",
        filename_source_key="id",
        keys_to_use=["id", "name", "pokemon"],
    )

    _extract_from_api_save_to_s3(
        endpoint="move",
        target_folder="moves",
        filename_source_key="id",
        keys_to_use=["id", "name", "learned_by_pokemon"],
    )

    _extract_from_api_save_to_s3(
        endpoint="generation",
        target_folder="generations",
        filename_source_key="id",
        keys_to_use=["id", "name", "pokemon_species", "types"],
    )

    _extract_from_api_save_to_s3(
        endpoint="pokemon-species",
        target_folder="pokemon_species",
        filename_source_key="id",
        keys_to_use=["id", "name", "varieties"],
    )


def check_for_new_generations(execution_date) -> None:
    """Do the check and download new generations data if needed."""
    generations_s3_prefix = f"{S3_TARGET_FOLDER}generations/"
    current_generations_count = len(list_s3_keys(generations_s3_prefix))

    generations_info_from_api = {}

    generation_endpoint = f"{POKE_API_URL}generation/"

    asyncio.run(
        make_api_requests(
            urls=[generation_endpoint],
            save_results_to_or_with=generations_info_from_api,
        )
    )

    api_generations_count = int(
        generations_info_from_api[generation_endpoint]["count"]
    )

    log_file_path = f"{LOG_S3_FOLDER_PATH}{execution_date}.txt"

    if api_generations_count <= current_generations_count:
        _log("No new generations!", log_file_path)
        return

    _log(
        "New generations available, "
        f"{api_generations_count - current_generations_count} "
        "will be downloaded",
        log_file_path,
    )

    _extract_from_api_save_to_s3(
        endpoint="generation",
        target_folder="generations",
        filename_source_key="id",
        keys_to_use=["id", "name", "pokemon_species"],
    )
