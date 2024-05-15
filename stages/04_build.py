from dataclasses import dataclass
from functools import cached_property
import json
from json_flattener import flatten, GlobalConfig, KeyConfig
import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import os

os.mkdir("brick")
schemas_path = Path("schemas")
raw_path = Path("raw")


def open_json_file(json_path: str):
    json_data = None
    with open(json_path, "r") as f:
        json_data = json.load(f)
    return json_data


json_paths = {
    "ndc": "drug-ndc-*",
    "drug_label": "drug-label-*",
    "drugs_fda": "drug-drugsfda-*",
}

# drugsfda['results']
# openfda column where not null
# keys: application_number, brand_name, generic_name, product_ndc, unii
# products column
# keys: active_ingredients


def read_json_data(paths):
    out = {}
    for k, v in paths.items():
        file_path = Path(raw_path).rglob(v)
        all = filter(os.path.isfile, file_path)
        out[k] = map(open_json_file, all)
    return out


JSON_DATA = read_json_data(json_paths)


@dataclass(frozen=True)
class DrugsFda:
    raw = JSON_DATA["drugs_fda"]
    cols: dict = {
        "openfda": [
            "brand_name",
            "application_number",
            "generic_name",
            "product_ndc",
            "unii",
        ]
    }

    @cached_property
    def get_raw(self):
        return list(self.raw)[0]["results"]

    def select_keys(self):
        return [
            {k: m["openfda"][k] for k in self.cols["openfda"] if k in m["openfda"]}
            for m in self.get_raw
            if "openfda" in m
        ]
        
    @cached_property
    def to_df(self):
        return pd.DataFrame.from_records(self.select_keys())
