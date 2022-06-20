import json

from src.airflow_dags.degtyarev_util.processing import \
    restore_original_pokemon_types


def test_with_past_types(snapshot):
    with open('test/processing/pokemon_35.json') as file:
        json_obj = json.load(file)
    
    result = restore_original_pokemon_types(json_obj=json_obj)
    
    snapshot.assert_match(result)


def test_without_past_types(snapshot):
    with open('test/processing/pokemon_1.json') as file:
        json_obj = json.load(file)
    
    result = restore_original_pokemon_types(json_obj=json_obj)
    
    snapshot.assert_match(result)
