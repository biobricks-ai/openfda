# JSON to Parquet Test Suite

This test suite comprehensively tests the `json2parquet.py` script to ensure it correctly converts JSON files to Parquet format, particularly for OpenFDA data structures.

## Test Coverage

### Core Functionality Tests (`test_json2parquet.py`)

1. **`TestSafeConvertToString`** - Tests the safe string conversion function:
   - Simple strings and numbers
   - Lists and dictionaries to JSON strings
   - Nested dictionaries
   - None values
   - Exception handling

2. **`TestFlattenComplexData`** - Tests DataFrame flattening:
   - Simple DataFrames (unchanged)
   - Complex data flattening to strings
   - Mixed object columns

3. **`TestConvertJsonToParquet`** - Tests the main conversion function:
   - Direct function calls with OpenFDA structure

4. **`TestJson2ParquetScript`** - Tests command-line script execution:
   - OpenFDA JSON structure with 'results' key
   - Simple JSON structure without 'results' key
   - Complex nested data structures
   - Empty results arrays
   - Invalid JSON handling
   - Missing input files

### Extended Tests (`test_json2parquet_extended.py`)

1. **`TestEdgeCases`** - Edge cases and boundary conditions:
   - Very large nested structures (100 levels deep)
   - Unicode and special characters
   - Numeric edge cases (infinity, NaN, etc.)
   - Empty containers (lists, dicts, sets, tuples)
   - Circular references
   - DataFrames with NaN values

2. **`TestPerformance`** - Performance with larger datasets:
   - Large dataset (1000 records)
   - Wide dataset (500+ columns)

3. **`TestDataTypeHandling`** - Various data type handling:
   - Boolean values and mixed boolean structures
   - Datetime-like strings
   - Null and missing values handling

4. **`TestErrorHandling`** - Error handling and recovery:
   - Malformed JSON structures
   - Empty JSON files
   - Permission denied scenarios

5. **`TestBackwardsCompatibility`** - Command-line compatibility:
   - Help message display
   - Realistic OpenFDA data structure processing

## Running Tests

### Prerequisites

Make sure you have the nix development environment set up:
```bash
nix develop
```

### Run All Tests
```bash
nix develop --command python3 -m pytest test/ -v
```

### Run Specific Test Files
```bash
# Core functionality tests only
nix develop --command python3 -m pytest test/test_json2parquet.py -v

# Extended tests only
nix develop --command python3 -m pytest test/test_json2parquet_extended.py -v
```

### Run Specific Test Classes
```bash
# Test only the safe string conversion functionality
nix develop --command python3 -m pytest test/test_json2parquet.py::TestSafeConvertToString -v

# Test only edge cases
nix develop --command python3 -m pytest test/test_json2parquet_extended.py::TestEdgeCases -v
```

### Run With Coverage (if available)
```bash
nix develop --command python3 -m pytest test/ --cov=stages --cov-report=html
```

## Test Structure

- **Unit Tests**: Test individual functions (`safe_convert_to_string`, `flatten_complex_data`)
- **Integration Tests**: Test the complete conversion process
- **System Tests**: Test command-line script execution
- **Performance Tests**: Test with large/complex datasets
- **Error Handling Tests**: Test failure scenarios

## Expected Behavior

The `json2parquet.py` script should:

1. **Handle OpenFDA Structure**: Correctly process JSON files with a 'results' key containing the data
2. **Fallback Processing**: Handle JSON files without 'results' key using direct normalization
3. **Complex Data Flattening**: Convert nested objects and arrays to JSON strings for Parquet compatibility
4. **Error Recovery**: Gracefully handle invalid JSON, missing files, and permission errors
5. **Data Type Preservation**: Maintain simple data types while converting complex structures to strings
6. **Unicode Support**: Properly handle Unicode characters and special symbols

## Known Behavior

- Boolean values may be converted to numpy boolean types by pandas
- Complex nested structures are flattened and converted to JSON strings
- Empty JSON objects `{}` create DataFrames with one row and zero columns
- Unicode characters may be escaped in JSON string representations
- Circular references are handled without infinite loops

## Adding New Tests

When adding new tests:

1. **Follow the naming convention**: `test_<description>`
2. **Use descriptive test names**: Clearly indicate what is being tested
3. **Include docstrings**: Explain the purpose of each test
4. **Use appropriate assertions**: Check for expected behavior, not just absence of errors
5. **Clean up resources**: Use `setup_method`/`teardown_method` for temporary files
6. **Test both success and failure cases**: Include edge cases and error conditions 