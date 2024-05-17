from dataclasses import dataclass, field
from functools import cached_property
import json
import pandas as pd
from pathlib import Path
import os
import toolz

if not os.path.exists("brick"):
    os.mkdir("brick")

# raw_path exists from previous stage
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


def read_json_data(paths):
    out = {}
    for k, v in paths.items():
        file_path = Path(raw_path).rglob(v)
        all = filter(os.path.isfile, file_path)
        out[k] = map(open_json_file, all)
    return out


JSON_DATA = read_json_data(json_paths)


def get_first_val(vs):
    if len(vs) == 1:
        return vs[0]
    else:
        return "|".join((val for val in vs))


@dataclass(frozen=True)
class DrugsFda:
    raw = JSON_DATA["drugs_fda"]
    cols: dict = field(
        default_factory=lambda: {
            "openfda": [
                "brand_name",
                "application_number",
                "generic_name",
                "product_ndc",
                "unii",
            ]
        }
    )

    @cached_property
    def get_raw(self):
        return list(self.raw)[0]["results"]

    def select_keys(self):
        return [
            {
                k: m["openfda"][k]
                for k in self.cols["openfda"]
                if k in m["openfda"]
            }
            for m in self.get_raw
            if "openfda" in m
        ]

    @cached_property
    def to_df(self) -> pd.DataFrame:
        m = [toolz.valmap(get_first_val, m) for m in self.select_keys()]
        return pd.DataFrame.from_records(m)

    def to_parquet(self, out_path: os.PathLike):
        self.to_df.to_parquet(out_path)


@dataclass(frozen=True)
class Substances:
    raw: dict = field(
        default_factory=lambda: open_json_file(
            "raw/other-substance-0001-of-0001/other-substance-0001-of-0001.json"
        )
    )
    
    @cached_property
    def get_raw(self):
        return self.raw
    
    def select_keys(self):
        raw = self.get_raw["results"]
        codes = [m["codes"] for m in raw if "codes" in m]
        cas_codes = [(list(filter(lambda m: m["code_system"] == "CAS", code))) for code in codes]
        return cas_codes
    
    @cached_property
    def to_df(self) -> pd.DataFrame:
        cas_codes = self.select_keys()
        dfs = [pd.DataFrame.from_records(c) for c in cas_codes]
        return pd.concat(dfs)
    
    def to_parquet(self, out_path: os.PathLike):
        self.to_df.to_parquet(out_path)
        

if __name__ == '__main__':
    fda = DrugsFda()
    print("Writing drugs_fda data to Parquet.")
    fda.to_parquet("brick/drugs_fda.parquet")

    sub = Substances()
    print("Writing other_substances data to Parquet.")
    sub.to_parquet("brick/other_substances.parquet")
