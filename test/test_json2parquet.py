import pytest
import pandas as pd
import json
import os
import sys
import tempfile
import subprocess
from pathlib import Path

# Add the stages directory to the path so we can import the functions
sys.path.insert(0, str(Path(__file__).parent.parent / "stages"))

# Import the functions we want to test
from json2parquet import safe_convert_to_string, flatten_complex_data, convert_json_to_parquet


class TestSafeConvertToString:
    """Test the safe_convert_to_string function"""
    
    def test_simple_string(self):
        """Test that simple strings are returned as-is"""
        result = safe_convert_to_string("hello")
        assert result == "hello"
    
    def test_simple_number(self):
        """Test that numbers are returned as-is"""
        result = safe_convert_to_string(42)
        assert result == 42
    
    def test_list_to_json(self):
        """Test that lists are converted to JSON strings"""
        test_list = [1, 2, 3, "hello"]
        result = safe_convert_to_string(test_list)
        assert result == '[1, 2, 3, "hello"]'
    
    def test_dict_to_json(self):
        """Test that dictionaries are converted to JSON strings"""
        test_dict = {"key1": "value1", "key2": 42}
        result = safe_convert_to_string(test_dict)
        expected = '{"key1": "value1", "key2": 42}'
        assert result == expected
    
    def test_nested_dict_to_json(self):
        """Test that nested dictionaries are handled correctly"""
        test_dict = {"outer": {"inner": "value"}}
        result = safe_convert_to_string(test_dict)
        expected = '{"outer": {"inner": "value"}}'
        assert result == expected
    
    def test_none_value(self):
        """Test that None values are handled"""
        result = safe_convert_to_string(None)
        assert result is None
    
    def test_exception_handling(self):
        """Test that exceptions are handled gracefully"""
        # Create an object that will raise an exception when converted
        class BadObject:
            def __iter__(self):
                raise Exception("This will fail")
        
        bad_obj = BadObject()
        result = safe_convert_to_string(bad_obj)
        # Should fall back to str() conversion
        assert isinstance(result, str)


class TestFlattenComplexData:
    """Test the flatten_complex_data function"""
    
    def test_simple_dataframe(self):
        """Test that simple dataframes are unchanged"""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        result = flatten_complex_data(df.copy())
        pd.testing.assert_frame_equal(result, df)
    
    def test_complex_data_flattening(self):
        """Test that complex data is flattened to strings"""
        df = pd.DataFrame({
            'simple': [1, 2, 3],
            'lists': [[1, 2], [3, 4], [5, 6]],
            'dicts': [{'a': 1}, {'b': 2}, {'c': 3}]
        })
        result = flatten_complex_data(df)
        
        # Simple column should remain unchanged
        assert result['simple'].tolist() == [1, 2, 3]
        
        # Lists should be converted to JSON strings
        assert result['lists'].iloc[0] == '[1, 2]'
        assert result['lists'].iloc[1] == '[3, 4]'
        
        # Dicts should be converted to JSON strings
        assert result['dicts'].iloc[0] == '{"a": 1}'
        assert result['dicts'].iloc[1] == '{"b": 2}'
    
    def test_mixed_object_column(self):
        """Test columns with mixed object types"""
        df = pd.DataFrame({
            'mixed': [
                "string",
                [1, 2, 3],
                {"key": "value"},
                42,
                None
            ]
        })
        result = flatten_complex_data(df)
        
        assert result['mixed'].iloc[0] == "string"
        assert result['mixed'].iloc[1] == '[1, 2, 3]'
        assert result['mixed'].iloc[2] == '{"key": "value"}'
        assert result['mixed'].iloc[3] == 42
        assert result['mixed'].iloc[4] is None


class TestConvertJsonToParquet:
    """Test the convert_json_to_parquet function directly"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_openfda_structure_direct(self):
        """Test convert_json_to_parquet function with OpenFDA structure"""
        # Create test JSON file with OpenFDA structure
        test_data = {
            "meta": {
                "disclaimer": "test disclaimer"
            },
            "results": [
                {
                    "id": "001",
                    "name": "Test Drug",
                    "manufacturer": "Test Company",
                    "ingredients": ["ingredient1", "ingredient2"]
                },
                {
                    "id": "002", 
                    "name": "Another Drug",
                    "manufacturer": "Another Company",
                    "complex_data": {
                        "nested": "value",
                        "number": 42
                    }
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "test_input.json")
        parquet_file = os.path.join(self.temp_dir, "test_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Call the function directly
        result_df = convert_json_to_parquet(json_file, parquet_file)
        
        assert os.path.exists(parquet_file), "Output parquet file was not created"
        assert len(result_df) == 2
        assert "id" in result_df.columns
        assert "name" in result_df.columns
        assert result_df.iloc[0]["id"] == "001"
        assert result_df.iloc[1]["id"] == "002"
        
        # Verify the saved parquet file
        df_loaded = pd.read_parquet(parquet_file)
        pd.testing.assert_frame_equal(result_df, df_loaded)


class TestJson2ParquetScript:
    """Test the complete json2parquet.py script via command line"""
    
    def setup_method(self):
        """Set up temporary files for each test"""
        self.temp_dir = tempfile.mkdtemp()
        self.script_path = Path(__file__).parent.parent / "stages" / "json2parquet.py"
    
    def teardown_method(self):
        """Clean up temporary files after each test"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_openfda_structure(self):
        """Test processing OpenFDA JSON structure with 'results' key"""
        # Create test JSON file with OpenFDA structure
        test_data = {
            "meta": {
                "disclaimer": "test disclaimer"
            },
            "results": [
                {
                    "id": "001",
                    "name": "Test Drug",
                    "manufacturer": "Test Company",
                    "ingredients": ["ingredient1", "ingredient2"]
                },
                {
                    "id": "002", 
                    "name": "Another Drug",
                    "manufacturer": "Another Company",
                    "complex_data": {
                        "nested": "value",
                        "number": 42
                    }
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "test_input.json")
        parquet_file = os.path.join(self.temp_dir, "test_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Run the script
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Script failed with error: {result.stderr}"
        assert os.path.exists(parquet_file), "Output parquet file was not created"
        
        # Verify the output
        df = pd.read_parquet(parquet_file)
        assert len(df) == 2
        assert "id" in df.columns
        assert "name" in df.columns
        assert df.iloc[0]["id"] == "001"
        assert df.iloc[1]["id"] == "002"
    
    def test_simple_json_structure(self):
        """Test processing simple JSON structure without 'results' key"""
        test_data = [
            {"field1": "value1", "field2": 100},
            {"field1": "value2", "field2": 200}
        ]
        
        json_file = os.path.join(self.temp_dir, "simple_input.json")
        parquet_file = os.path.join(self.temp_dir, "simple_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Run the script
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Script failed with error: {result.stderr}"
        assert os.path.exists(parquet_file), "Output parquet file was not created"
        
        # Verify the output
        df = pd.read_parquet(parquet_file)
        assert len(df) == 2
        assert df.iloc[0]["field1"] == "value1"
        assert df.iloc[1]["field2"] == 200
    
    def test_complex_nested_data(self):
        """Test processing JSON with heavily nested and complex data structures"""
        test_data = {
            "results": [
                {
                    "id": "complex_001",
                    "nested_list": [
                        {"inner_id": 1, "inner_value": "test1"},
                        {"inner_id": 2, "inner_value": "test2"}
                    ],
                    "nested_dict": {
                        "level1": {
                            "level2": {
                                "level3": "deep_value"
                            }
                        }
                    },
                    "mixed_array": [1, "string", {"key": "value"}, [1, 2, 3]]
                }
            ]
        }
        
        json_file = os.path.join(self.temp_dir, "complex_input.json")
        parquet_file = os.path.join(self.temp_dir, "complex_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Run the script
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Script failed with error: {result.stderr}"
        assert os.path.exists(parquet_file), "Output parquet file was not created"
        
        # Verify the output - complex data should be stringified
        df = pd.read_parquet(parquet_file)
        assert len(df) == 1
        assert df.iloc[0]["id"] == "complex_001"
        
        # Check that complex data was converted to strings
        nested_list = df.iloc[0]["nested_list"]
        assert isinstance(nested_list, str)
        assert "inner_id" in nested_list  # Should contain the original data as JSON string
    
    def test_empty_results(self):
        """Test processing JSON with empty results array"""
        test_data = {
            "meta": {"total": 0},
            "results": []
        }
        
        json_file = os.path.join(self.temp_dir, "empty_input.json")
        parquet_file = os.path.join(self.temp_dir, "empty_output.parquet")
        
        with open(json_file, 'w') as f:
            json.dump(test_data, f)
        
        # Run the script
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode == 0, f"Script failed with error: {result.stderr}"
        assert os.path.exists(parquet_file), "Output parquet file was not created"
        
        # Verify the output
        df = pd.read_parquet(parquet_file)
        assert len(df) == 0  # Should be empty but valid parquet file
    
    def test_invalid_json(self):
        """Test handling of invalid JSON files"""
        json_file = os.path.join(self.temp_dir, "invalid_input.json")
        parquet_file = os.path.join(self.temp_dir, "invalid_output.parquet")
        
        # Create invalid JSON file
        with open(json_file, 'w') as f:
            f.write("{invalid json content")
        
        # Run the script - should fail gracefully
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode != 0, "Script should fail with invalid JSON"
        assert not os.path.exists(parquet_file), "Output file should not be created with invalid input"
    
    def test_missing_input_file(self):
        """Test handling of missing input files"""
        json_file = os.path.join(self.temp_dir, "nonexistent.json")
        parquet_file = os.path.join(self.temp_dir, "output.parquet")
        
        # Run the script with non-existent input file
        result = subprocess.run([
            sys.executable, str(self.script_path), json_file, parquet_file
        ], capture_output=True, text=True)
        
        assert result.returncode != 0, "Script should fail with missing input file"
        assert not os.path.exists(parquet_file), "Output file should not be created with missing input"


if __name__ == "__main__":
    pytest.main([__file__]) 