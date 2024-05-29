from abc import abstractmethod
from functools import cached_property
import json
import pandas as pd
from pathlib import Path
import os
import toolz
from typing import Protocol

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
    "drugs_nsde": "other-nsde-*",
    "unii": "other-unii-*",
    "substances": "other-substance-*"
}


def read_json_data(paths):
    out = {}
    for k, v in paths.items():
        file_path = Path(raw_path).rglob(v)
        all = filter(os.path.isfile, file_path)
        out[k] = map(open_json_file, all)
    return out


def get_first_val(vs):
    if len(vs) == 1:
        return vs[0]
    else:
        if isinstance(vs, list):
            return "|".join((val for val in vs))
        else: return vs


class OpenFDA(Protocol):
    raw: dict = read_json_data(json_paths)
    
    @cached_property
    def get_raw(self) -> list:
        return self.raw
    
    @cached_property
    def has_multiple(self) -> bool:
        return toolz.count(self.get_raw) > 1
    
    def select_keys(self) -> list:
        if self.has_multiple:
            return [m["results"] for m in self.get_raw]
        else:
            return self.get_raw[0]["results"]
    
    @cached_property
    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        raise NotImplementedError
    
    def to_parquet(self, out_path):
        try:
            self.to_df.to_parquet(out_path)
        except Exception as e:
            print(e)


class DrugsFda(OpenFDA):

    @cached_property
    def get_raw(self):
        return list(super().get_raw["drugs_fda"])

    def __call__(self, out_path):
        print("Writing drugs_fda data to Parquet.")
        super().to_parquet(out_path)


class Substances(OpenFDA):

    @cached_property
    def get_raw(self):
        return list(super().get_raw["substances"])

    def __call__(self, out_path):
        print("Writing other_substances data to Parquet.")
        super().to_parquet(out_path)

def cleanup_labels(result):
    out = {}
    for k, v in result.items():
        if k != "effective_time":
            if len(v) >= 1 and isinstance(v, list):
                out[k] = ",".join((val for val in v))
            elif len(v) == 1:
                out[k] = v
            else:
                out[k] = v
        else:
            out[k] = v
    return out

class DrugsLabel(OpenFDA):

    @cached_property
    def get_raw(self):
        return list(super().get_raw["drug_label"])

    @cached_property
    def to_df(self):
        ks = [[cleanup_labels(result) for result in lst] for lst in self.select_keys()]
        dfs = [pd.DataFrame.from_records(m) for m in ks]
        return pd.concat(dfs)
        
    def __call__(self, out_path):
        print("Writing labels data to Parquet.")
        super().to_parquet(out_path)
    
class NDC(OpenFDA):

    @cached_property
    def get_raw(self):
        return list(super().get_raw["ndc"])

    def select_keys(self):
        return super().select_keys()
    
    @cached_property
    def to_df(self):
        return pd.DataFrame.from_records(self.select_keys())
    
    def to_parquet(self, out_path: os.PathLike):
        super().to_parquet(out_path)
        
class UNII(OpenFDA):
    
    @cached_property
    def get_raw(self):
        return list(super().get_raw["unii"])
    
    def __call__(self, out_path):
        print("Writing UNII data to Parquet.")
        super().to_parquet(out_path)
        
class NSDE(OpenFDA):
    
    @cached_property
    def get_raw(self):
        return list(super().get_raw["drugs_nsde"])
    
    def select_keys(self):
        return super().select_keys()
    
    @cached_property
    def to_df(self):
        return pd.DataFrame.from_records(self.select_keys())
    
    def to_parquet(self, out_path: os.PathLike):
        super().to_parquet(out_path)

if __name__ == "__main__":
    fda = DrugsFda()
    fda("brick/drugs_fda.parquet")
    
    # ndc = NDC() not working right now...
    # print("Writing NDC data to Parquet.")
    # ndc.to_parquet("brick/test_ndc.parquet")
    
    unii = UNII()
    unii("brick/unii.parquet")

    sub = Substances()
    sub("brick/other_substances.parquet")

    labels = DrugsLabel()
    labels("brick/drug_labels.parquet")