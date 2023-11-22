import json
import logging
import os
import pandas as pd

def load_json_config_file(file_path) -> dict:
    with open(file_path, 'r') as f:
        return json.load(f)

def load_file_to_dataframe(input_file:str, file_type:str) -> pd.DataFrame:
    """
    Load the input file as a Pandas DataFrame.

    Parameters
    - input_file: Local Path to the input file.

    Returns
    - file_df: Pandas DataFrame containing the input file.
    """

    # Check if the Input file exists
    if not os.path.exists(input_file):
        logging.error(f"Input file {input_file} does not exist.")
        raise FileNotFoundError(f"Input file {input_file} does not exist.")

    # Check the file format
    if input_file.endswith('.xlsx'):
        logging.info(f"Loading Input file {input_file} as an Excel file.")
        Input_file_df = pd.read_excel(input_file)
    elif input_file.endswith('.csv'):
        logging.info(f"Loading Input file {input_file} as a CSV file.")
        Input_file_df = pd.read_csv(input_file)
    else:
        logging.error("Unsupported file format. Only .xlsx and .csv are supported.")
        raise ValueError("Unsupported file format. Only .xlsx and .csv are supported.")
    
    # Check if the data frame is empty
    if Input_file_df.empty:
        logging.error(f"Input file {input_file} is empty.")
        raise ValueError("Input file is empty")
    else:
        logging.info(f"Input file '{input_file}' loaded successfully.")
    
    return Input_file_df


