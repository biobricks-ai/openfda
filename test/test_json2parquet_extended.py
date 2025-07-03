import pytest
import pandas as pd
import json
import os
import sys
import tempfile
import subprocess
import numpy as np
from pathlib import Path

# Add the stages directory to the path so we can import the functions
sys.path.insert(0, str(Path(__file__).parent.parent / "stages"))

from json2parquet import safe_convert_to_string, flatten_complex_data, convert_json_to_parquet


class TestEdgeCases:
    """Test edge cases and boundary conditions"""
    
    def test_very_large_nested_structures(self):
        """Test handling of very large nested structures"""
        # Create a deeply nested dictionary
        nested_dict = {"level": 1}
        current = nested_dict
        for i in range(2, 101):  # 100 levels deep
            current["next"] = {"level": i}
            current = current["next"]
        
        result = safe_convert_to_string(nested_dict)
        assert isinstance(result, str)
        assert "level" in result
        assert "100" in result
    
    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters"""
        test_data = {
            "unicode": "测试数据",
            "emoji": "🚀 🐍 📊",
            "special_chars": "!@#$%^&*()[]{}|\\:;\"'<>?,./",
            "newlines": "line1\nline2\ttab\rcarriage_return"
        }
        
        result = safe_convert_to_string(test_data)
        assert isinstance(result, str)
        # JSON encoding may use Unicode escapes, so check for the presence of data
        assert "unicode" in result
        assert "emoji" in result
        assert "special_chars" in result
        assert "newlines" in result
    
    def test_numeric_edge_cases(self):
        """Test various numeric edge cases"""
        test_cases = [
            float('inf'),
            float('-inf'),
            0,
            -0,
            1e-10,
            1e10,
            np.nan if 'numpy' in sys.modules else None
        ]
        
        for case in test_cases:
            if case is not None:
                result = safe_convert_to_string(case)
                # Should either return as-is for simple numbers or convert to string
                assert result is not None
    
    def test_empty_containers(self):
        """Test empty lists, dicts, etc."""
        test_cases = [
            ([], "[]"),
            ({}, "{}"),
            (set(), "set()"),  # Sets are converted to str
            (tuple(), "()"),   # Tuples are converted to str
            ("", "")           # Empty strings remain as-is
        ]
        
        for case, expected in test_cases:
            result = safe_convert_to_string(case)
            if isinstance(case, (list, dict)):
                assert isinstance(result, str)
                assert result == expected
            elif isinstance(case, (set, tuple)):
                # These should be converted to string representation
                assert isinstance(result, str)
            else:
                assert result == case
    
    def test_circular_references(self):
        """Test handling of circular references (should not hang)"""
        # Create a circular reference
        circular_dict = {"key": "value"}
        circular_dict["self"] = circular_dict
        
        # This should not hang and should return a string
        result = safe_convert_to_string(circular_dict)
        assert isinstance(result, str)
    
    def test_dataframe_with_nan_values(self):
        """Test DataFrames containing NaN values"""
        df = pd.DataFrame({
            'numbers': [1, 2, np.nan, 4],
            'strings': ['a', 'b', None, 'd'],
            'mixed': [1, 'text', np.nan, {'key': 'value'}]
        })
        
        result = flatten_complex_data(df)
        assert len(result) == 4
        # NaN and None should be preserved where possible
        assert pd.isna(result['numbers'].iloc[2])
        assert result['strings'].iloc[2] is None


class TestPerformance:
    """Test performance with larger datasets"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_large_dataset(self):
        """Test processing a large dataset"""
        # Create a large dataset with 1000 records
        large_data = {
            "results": [
                {
                    "id": f"record_{i}",
                    "value": i * 1.5,
                    "description": f"This is record number {i} with some text",
                    "tags": [f"tag_{j}" for j in range(i % 10)],
                    "metadata": {
                        "created": f"2024-01-{(i % 28) + 1:02d}",
                        "category": f"cat_{i % 5}",
                        "nested": {
                            "level1": f"value_{i}",
                            "level2": {"deep": i ** 2}
                        }
                    }
                }
                for i in range(1000)
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "large_input.json")
        parquet_file = os.path.join(self.temp_dir, "large_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(large_data, f)
        
        # This should complete without errors
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert len(result_df) == 1000
        assert os.path.exists(parquet_file)
        
        # Verify the file can be read back
        df_loaded = pd.read_parquet(parquet_file)
        assert len(df_loaded) == 1000
    
    def test_wide_dataset(self):
        """Test processing a dataset with many columns"""
        # Create a dataset with many columns
        wide_record = {f"field_{i}": f"value_{i}" for i in range(500)}
        wide_record["complex_field"] = {"nested": [1, 2, 3, 4, 5]}
        
        wide_data = {
            "results": [wide_record]
        }
        
        json_file = os.path.join(self.temp_dir, "wide_input.json")
        parquet_file = os.path.join(self.temp_dir, "wide_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(wide_data, f)
        
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert len(result_df.columns) >= 500
        assert len(result_df) == 1
        assert os.path.exists(parquet_file)


class TestDataTypeHandling:
    """Test handling of various data types"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_boolean_values(self):
        """Test handling of boolean values"""
        test_data = {
            "results": [
                {
                    "id": "bool_test",
                    "flag_true": True,
                    "flag_false": False,
                    "bool_list": [True, False, True],
                    "mixed_bool": {"enabled": True, "disabled": False}
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "bool_input.json")
        parquet_file = os.path.join(self.temp_dir, "bool_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        # Pandas may convert Python bool to numpy bool, so check truth value
        assert bool(result_df.iloc[0]["flag_true"]) is True
        assert bool(result_df.iloc[0]["flag_false"]) is False
        # Complex boolean structures should be stringified
        assert isinstance(result_df.iloc[0]["bool_list"], str)
        # Check that nested booleans are flattened properly
        assert "mixed_bool.enabled" in result_df.columns
        assert bool(result_df.iloc[0]["mixed_bool.enabled"]) is True
    
    def test_datetime_strings(self):
        """Test handling of datetime-like strings"""
        test_data = {
            "results": [
                {
                    "id": "datetime_test",
                    "iso_date": "2024-01-15T10:30:00Z",
                    "simple_date": "2024-01-15",
                    "timestamp": "1705312200",
                    "date_array": ["2024-01-01", "2024-01-02", "2024-01-03"]
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "datetime_input.json")
        parquet_file = os.path.join(self.temp_dir, "datetime_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        # Date strings should be preserved as strings
        assert result_df.iloc[0]["iso_date"] == "2024-01-15T10:30:00Z"
        assert result_df.iloc[0]["simple_date"] == "2024-01-15"
        # Arrays should be stringified
        assert isinstance(result_df.iloc[0]["date_array"], str)
        assert "2024-01-01" in result_df.iloc[0]["date_array"]
    
    def test_null_and_missing_values(self):
        """Test handling of null and missing values"""
        test_data = {
            "results": [
                {
                    "id": "null_test",
                    "null_field": None,
                    "missing_field": None,
                    "empty_string": "",
                    "zero_value": 0,
                    "false_value": False,
                    "mixed_nulls": [None, "", 0, False, "actual_value"]
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "null_input.json")
        parquet_file = os.path.join(self.temp_dir, "null_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert result_df.iloc[0]["null_field"] is None
        assert result_df.iloc[0]["empty_string"] == ""
        assert result_df.iloc[0]["zero_value"] == 0
        # Pandas may convert Python bool to numpy bool
        assert bool(result_df.iloc[0]["false_value"]) is False
        # Array with mixed nulls should be stringified
        assert isinstance(result_df.iloc[0]["mixed_nulls"], str)


class TestErrorHandling:
    """Test error handling and recovery"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_malformed_json_structure(self):
        """Test handling of malformed but parseable JSON"""
        # JSON that parses but has unexpected structure
        test_data = {
            "unexpected_root": "value",
            "no_results_key": [1, 2, 3],
            "string_instead_of_object": "this is a string"
        }
        
        json_file = os.path.join(self.temp_dir, "malformed_input.json")
        parquet_file = os.path.join(self.temp_dir, "malformed_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Should not crash, but use fallback normalization
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert os.path.exists(parquet_file)
        assert len(result_df) >= 0  # Should at least not crash
    
    def test_empty_json_file(self):
        """Test handling of empty JSON files"""
        test_data = {}
        
        json_file = os.path.join(self.temp_dir, "empty_input.json")
        parquet_file = os.path.join(self.temp_dir, "empty_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Should handle empty JSON gracefully
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert os.path.exists(parquet_file)
        # Empty JSON {} creates a DataFrame with one row and no columns
        assert len(result_df.columns) == 0
        assert len(result_df) == 1  # One empty row
    
    def test_permission_denied_output(self):
        """Test handling when output file cannot be written"""
        test_data = {"results": [{"id": "test"}]}
        
        json_file = os.path.join(self.temp_dir, "test_input.json")
        # Try to write to a directory that doesn't exist
        parquet_file = "/root/cannot_write_here.parquet"  # Should fail
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Should raise an exception
        with pytest.raises(Exception):
            convert_json_to_parquet(json_file, parquet_file)


class TestBackwardsCompatibility:
    """Test that the script still works as expected from command line"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
        self.script_path = Path(__file__).parent.parent / "stages" / "json2parquet.py"
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_command_line_usage_help(self):
        """Test that script shows help when called with wrong arguments"""
        # Call script with no arguments
        result = subprocess.run([
            sys.executable, str(self.script_path)
        ], capture_output=True, text=True)
        
        assert result.returncode != 0
        assert "Usage:" in result.stdout or "Usage:" in result.stderr
    
    def test_command_line_with_actual_openfda_structure(self):
        """Test with a structure that mimics real OpenFDA data"""
        # Simulate a more realistic OpenFDA response
        openfda_data = {
            "meta": {
                "disclaimer": "Do not rely on openFDA to make decisions regarding medical care.",
                "terms": "https://open.fda.gov/terms/",
                "license": "https://open.fda.gov/license/",
                "last_updated": "2024-01-15",
                "results": {
                    "skip": 0,
                    "limit": 100,
                    "total": 2
                }
            },
            "results": [
                {
                    "safetyreportid": "12345678",
                    "safetyreportversion": "1",
                    "receivedate": "20240115",
                    "receiptdate": "20240115",
                    "serious": "1",
                    "transmissiondate": "20240116",
                    "patient": {
                        "patientonsetage": "65",
                        "patientonsetageunit": "801",
                        "patientsex": "2",
                        "drug": [
                            {
                                "drugcharacterization": "1",
                                "medicinalproduct": "ASPIRIN",
                                "drugdosagetext": "100 MG, DAILY",
                                "drugstartdate": "20240101",
                                "activesubstance": {
                                    "activesubstancename": "ASPIRIN"
                                }
                            }
                        ],
                        "reaction": [
                            {
                                "reactionmeddrapt": "Headache",
                                "reactionoutcome": "1"
                            }
                        ]
                    }
                },
                {
                    "safetyreportid": "87654321",
                    "safetyreportversion": "1",
                    "receivedate": "20240114",
                    "serious": "2",
                    "patient": {
                        "patientonsetage": "45",
                        "patientsex": "1",
                        "drug": [
                            {
                                "drugcharacterization": "1",
                                "medicinalproduct": "IBUPROFEN",
                                "drugdosagetext": "200 MG, TWICE DAILY"
                            }
                        ],
                        "reaction": [
                            {
                                "reactionmeddrapt": "Nausea",
                                "reactionoutcome": "2"
                            }
                        ]
                    }
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "openfda_realistic.json")
        parquet_file = os.path.join(self.temp_dir, "openfda_realistic.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(openfda_data, f)
        
        # Run the script
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Script failed: {result.stderr}"
        assert os.path.exists(parquet_file)
        
        # Verify the structure is preserved correctly
        df = pd.read_parquet(parquet_file)
        assert len(df) == 2
        assert "safetyreportid" in df.columns
        assert df.iloc[0]["safetyreportid"] == "12345678"
        assert df.iloc[1]["safetyreportid"] == "87654321"
        
        # Complex nested structures should be stringified
        assert isinstance(df.iloc[0]["patient.drug"], str)
        assert "ASPIRIN" in df.iloc[0]["patient.drug"]


if __name__ == "__main__":
    pytest.main([__file__]) 