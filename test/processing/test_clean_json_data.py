import json

from src.airflow_dags.degtyarev_util.processing import clean_json_data


def test_clean_json_data(snapshot):
    with open("test/processing/generation_1.json") as file:
        generation = json.load(file)
    keys_to_use = ["id", "name", "pokemon_species", "types"]
    snapshot.assert_match(
        clean_json_data(json_obj=generation, keys_to_use=keys_to_use)
    )
