import pandas as pd
import sys
import json
import pyarrow as pyarrow
import fastparquet as fastparquet

def safe_convert_to_string(x):
    """Safely convert complex objects to strings"""
    try:
        if isinstance(x, (list, dict)):
            return json.dumps(x)
        elif hasattr(x, '__iter__') and not isinstance(x, str):
            # Handle other iterable types (like numpy arrays)
            return str(x)
        else:
            return x
    except:
        return str(x)

def flatten_complex_data(df):
    """Convert complex nested data types to strings to handle PyArrow limitations"""
    for col in df.columns:
        if df[col].dtype == 'object':
            # Apply conversion to all values in the column
            df[col] = df[col].apply(safe_convert_to_string)
    return df

def convert_json_to_parquet(input_filename, output_filename):
    """Convert a JSON file to Parquet format"""

    # Read JSON file and extract the results array
    with open(input_filename, 'r') as f:
        data = json.load(f)

    # OpenFDA files have a structure with 'results' containing the actual data
    if 'results' in data:
        DF = pd.json_normalize(data['results'])
    else:
        # Fallback for other JSON structures
        DF = pd.json_normalize(data)

    # Handle complex nested data
    DF = flatten_complex_data(DF)

    DF.to_parquet(output_filename)
    
    return DF

def main():
    """Main function for command-line usage"""
    if len(sys.argv) != 3:
        print("Usage: python json2parquet.py <input_json_file> <output_parquet_file>")
        sys.exit(1)
    
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    
    try:
        convert_json_to_parquet(input_filename, output_filename)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
