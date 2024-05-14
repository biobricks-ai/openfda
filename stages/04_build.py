import json
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from itertools import chain, islice
import os

os.mkdir("brick")
schemas_path = Path("schemas")


def open_schema_file(filename: str):
    schema = None
    with open(schemas_path / filename, "r") as f:
        schema = json.load(f)
    return schema


animal_vet_schema = open_schema_file("animalandveterinarydrugevent_schema.json")

def flatten_schema(schema: dict):
    props: dict = schema["properties"]
    props_types = {
                key: {k: v["type"] for k, v in val["properties"].items() if "type" in v}
                for key, val in props.items()
                if "properties" in val
            }
    flat_types = {k: v["type"] for k, v in props.items() if "type" in v}
    props_types |= flat_types
    out = {k: "string" for k, v in props_types.items() if isinstance(v, dict)}
    props_types |= out
    return props_types


def make_pd_compliant_schema(schema):
    flattened = flatten_schema(schema)
    return [{'name': k, 'data': v} for k, v in flattened]

schema = flatten_schema(animal_vet_schema)

def open_json_file(json_path: str):
    json_data = None
    with open(json_path, "r") as f:
        json_data = json.load(f)
    return json_data
    

def merge_json_with_schema(schema_path: str, json_path: str) -> dict:
    schema = open_schema_file(schema_path)
    json_data = open_json_file(json_path)
    json_data['schema'] = schema
    json_data['schema']['fields'] = [{'name': k, 'type': v} for k, v in flatten_schema(schema).items()]
    json_data['data'] = json_data['results']
    del json_data['results']
    return json.dumps(json_data)

data = merge_json_with_schema("animalandveterinarydrugevent_schema.json", "raw/animalandveterinary-event-0001-of-0001.json/animalandveterinary-event-0001-of-0001.json")

pd.read_json(data, orient="table",)

animal_schema = flatten_schema(open_schema_file("animalandveterinarydrugevent_schema.json"))

[{'name': k ,'type': v} for k, v in animal_schema.items()]