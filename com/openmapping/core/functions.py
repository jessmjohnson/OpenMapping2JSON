import copy
import logging
import json
import os
import pandas as pd

from const import OUTPUT_FOLDER
from maps import MAP_FILE_REQUIRED_COLUMNS_MAP, TARGET_SOURCE_AREA_MAP, PANDAS_DATATYPE_MAP
from utils.azure_utils import get_secrets_from_keyvault
from utils.database_utils import get_sql_table_schema
from utils.file_utils import load_json_config_file, load_file_to_dataframe
from utils.functions import cleanse_column_list

def get_required_columns_list(target_data_area: str) -> list:

    # Check if the provided target_data_area is a valid key
    if target_data_area in MAP_FILE_REQUIRED_COLUMNS_MAP:
        required_columns = MAP_FILE_REQUIRED_COLUMNS_MAP[target_data_area]
        return required_columns
    else:
        # Raise a custom exception for an invalid target data area
        raise ValueError(f"Invalid target data area: {target_data_area}. Valid options are {list(MAP_FILE_REQUIRED_COLUMNS_MAP.keys())}")

def get_data_area_audit_columns(target_data_area: str) -> list:
    """
    Get the audit columns for the target data area.

    Parameters:
    - target_data_area: Target data area for the mapping file.

    Returns:
    - audit_columns: List of dictionaries containing the audit columns.
    """
    
    try:
        # Load mint_data_areas.json file and get audit_columns
        mint_data_areas_filepath = os.path.join(OUTPUT_FOLDER, "mint", "mint_data_areas.json")
        mint_data_areas_config = load_json_config_file(mint_data_areas_filepath)

        # Get audit_columns from mint_data_areas.json
        data_area_config = mint_data_areas_config.get(target_data_area, {})

        if not data_area_config:
            raise ValueError(f"Data area '{target_data_area}' not found in mint_data_areas.json")

        audit_columns = data_area_config.get('audit_columns', [])

        if not isinstance(audit_columns, list):
            raise ValueError("Audit columns in mint_data_areas.json is not a list")

        # Create audit_columns list of dictionaries with 'name' and 'type' from audit_columns
        audit_columns = [{"name": col['name'], "type": col['type']} for col in audit_columns]

        return audit_columns
    except Exception as e:
        # Handle the exception, log an error message, and potentially raise or return an error indicator
        error_message = f"Error getting data area audit columns: {str(e)}"
        logging.error(error_message)
        raise ValueError(error_message)

def get_target_table_list(mapping_file_df: pd.DataFrame) -> list:
    """
    Get the list of target tables from the mapping file.

    Parameters:
    - mapping_file_df: Pandas DataFrame containing the mapping file.

    Returns:
    - target_tables_list: List of target tables.
    """
    # Get the list of unique target tables from the mapping file
    target_tables_list = mapping_file_df['Target Table Name'].unique().tolist()
    target_tables_list = cleanse_column_list(target_tables_list)

    return target_tables_list

def write_config_file(domain: str, target_data_area: str, file_name: str, config_json: dict) -> None:
    """
    Write the config file to the output folder.

    Parameters:
    - domain: Domain in which files are to be stored.
    - target_data_area: Target data area for the mapping file.
    - file_name: Name of the config file.
    - config_json: Dictionary containing the config file.
    """

    # Create the output folder if it does not exist
    output_folder = os.path.join(OUTPUT_FOLDER, 'domains', domain, target_data_area)
    try:
        os.makedirs(output_folder, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creating output folder '{output_folder}': {str(e)}")
        raise OSError(f"Error creating output folder '{output_folder}': {str(e)}")

    # Write the config file
    file_path = os.path.join(output_folder, file_name)
    try:
        with open(file_path, 'w') as target_file:
            json.dump(config_json, target_file, indent=4)
    except Exception as e:
        logging.error(f"Error writing to file '{file_path}': {str(e)}")
        raise Exception(f"Error writing to file '{file_path}': {str(e)}")

    logging.info(f"Config file '{file_path}' generated successfully.")

def generate_source_schema_config(domain: str, target_data_area: str, source_table_name: str) -> dict:
    """
    Generate the source schema for the mapping file.

    Parameters:
    - domain: Domain in which files are to be stored.
    - target_data_area: Target data area for the mapping file.
    - source_table_name: Name of the source table.

    Returns:
    - source_schema_dict: Dictionary containing the source schema.
    """

    # Initialize Variables
    source_schema_dict = {}
    source_data_area = TARGET_SOURCE_AREA_MAP.get(target_data_area)

    logging.info(f"Generating '{source_data_area}' source schema for {source_table_name}...")

    if source_data_area == 'landing-zone':
        connection_string = get_secrets_from_keyvault('mintetldevkeyvaulteus', 'cura-sqlserver-rdsoprd-cxnstr-with-driver')
        source_schema_cols_dict = get_sql_table_schema(connection_string, source_table_name)

        source_schema_dict= {
            "domain": domain,
            "table_name": source_table_name,
            "columns": source_schema_cols_dict
        }

    elif source_data_area in ('bronze','silver'):
        source_file_name = f"{source_data_area}_schema_{source_table_name}.json"
        source_schema_filepath = os.path.join(OUTPUT_FOLDER, "domains", domain, source_data_area, source_file_name)
        source_schema_dict = load_json_config_file(source_schema_filepath)
    else:
        logging.error(f"Invalid target data area: {target_data_area}. Expected values are {', '.join(TARGET_SOURCE_AREA_MAP.keys())}")
        raise ValueError(f"Invalid target data area: {target_data_area}")

    return source_schema_dict

def validate_mapping_file(mapping_file:str, domain:str, target_data_area:str, file_type:str) -> pd.DataFrame:
    """
    Validate the mapping file before generating the schema and mapping files.

    Parameters
    - mapping_file: Local Path to the mapping file.
    - domain: Domain in which files are to be stored.
    - target_data_area: Target data area for the mapping file.
    - file_type: Type of the mapping file (excel/csv).

    Returns
    - mapping_file_df: Pandas DataFrame containing the mapping file.
    """

    # Initialize Variables
    missing_columns_list = []
    invalid_source_columns_list = []

    # Load the mapping file as a Pandas DataFrame for further processing
    logging.info(f"Validating mapping file '{mapping_file}'...")
    mapping_file_df = load_file_to_dataframe(mapping_file, file_type)

    # Get the required columns for the target data area
    logging.debug(f">>> Validating mapping file required columns...")
    required_columns_list = get_required_columns_list(target_data_area)

    # Check if the mapping file contains all the required columns
    missing_columns_list = set(required_columns_list) - set(mapping_file_df.columns)
    if missing_columns_list:
        logging.error(f"Mapping file `{mapping_file}` is missing required columns: {missing_columns_list}")
        raise ValueError(f"Mapping file `{mapping_file}` is missing the following required columns: {missing_columns_list}")
    else:
        logging.info(">>> All required columns are present in the mapping file.")

    # Check if the source columns exist in the source data store
    logging.debug(">>> Validating the existence of source columns in the source schema ...")
    
    # Get the list of unique source tables from the mapping file
    source_tables_list = mapping_file_df['Source Table Name'].unique().tolist()
    source_tables_list = cleanse_column_list(source_tables_list)

    # Create a dictionary to store the source tables and their corresponding schema_dict from the source_tables_list
    source_tables_schema_dict = {
        source_table: generate_source_schema_config(domain, target_data_area, source_table)
        for source_table in source_tables_list
    }

    # Iterate through the mapping_file_df and check source columns in source tables
    invalid_source_columns_list = []

    # Get the list of unique source columns from the mapping file for each source table
    for source_table in source_tables_list:
        source_columns_list = mapping_file_df[mapping_file_df['Source Table Name'] == source_table]['Source Column Name'].unique().tolist()
        source_columns_list = cleanse_column_list(source_columns_list)

        # Get a list of columns source columns from the source schema
        source_schema_columns_list = [col['name'] for col in source_tables_schema_dict[source_table]['columns']]

        # Check if the the source columns exist in the source table schema
        invalid_source_columns_list += [col for col in source_columns_list if col not in source_schema_columns_list]
     
    # Check if there are any invalid source columns
    if invalid_source_columns_list:
        logging.error(f"Mapping file `{mapping_file}` contains invalid source columns: {invalid_source_columns_list}")
        raise ValueError(f"Mapping file `{mapping_file}` contains invalid source columns: {invalid_source_columns_list}")
    else:
        logging.info(">>> All source schema columns exist in the source data store.")

    return mapping_file_df, source_tables_schema_dict

def generate_target_table_config(target_data_area:str, mapping_file_df: pd.DataFrame) -> dict:
    """
    Generate the target tables config for the mapping file.

    Parameters:
    - target_data_area: Target data area for the mapping file.
    - mapping_file_df: Pandas DataFrame containing the mapping file.

    Returns:
    - target_tables_config: Dictionary containing the target tables config.
    """

    try:
        # Get the processing action based on the target data area
        processing_action = 'ingest' if target_data_area in ('landing-zone', 'bronze') else 'merge'

        # Get the list of unique target tables from the mapping file
        target_tables_list = mapping_file_df['Target Table Name'].unique().tolist()
        target_tables_list = cleanse_column_list(target_tables_list)

        # Create the target tables config
        target_tables_config = {
            "action": processing_action,
            "target_data_area": target_data_area,
            "config_storage_account": "mintetl[env]configeus",
            "key_vault_name": "mintetl[env]keyvaulteus",
            "target_table_list": target_tables_list
        }

        return target_tables_config

    except Exception as e:
        # Handle the exception, log an error message, and potentially raise or return an error indicator
        error_message = f"Error generating target table config: {str(e)}"
        logging.error(error_message)
        raise ValueError(error_message)

def generate_target_schema_config(domain:str, target_data_area:str, source_schema_dict:dict, mapping_file_df: pd.DataFrame) -> dict:
    """
    Generate the target schema for the mapping file.

    Parameters:
    - domain: Domain in which files are to be stored.
    - target_data_area: Target data area for the mapping file.
    - source_schema_dict: Dictionary containing the source schema.
    - mapping_file_df: Pandas DataFrame containing the mapping file.

    Returns:
    - target_tables_schema_dict: Dictionary containing the target schema.
    """

    try: 
        target_tables_schema_dict = {}

        target_table_list = mapping_file_df['Target Table Name'].unique().tolist()
        target_table_list = cleanse_column_list(target_table_list)

        # Iterate through the mapping_file_df and create the target schema for each target table
        for target_table_name in target_table_list:
            
            logging.debug(f">>> Generating target schema for {target_table_name}...")

            # Create the target schema columns
            target_column_schema_list = []

            # Get the source table name for the current target table from the mapping file
            source_table_name = mapping_file_df[mapping_file_df['Target Table Name'] == target_table_name]['Source Table Name'].unique().tolist()[0]

            # Get the source schema for the current target table from the source schema dictionary
            curr_source_schema_dict = source_schema_dict[source_table_name]

            # Get the table mappings for the current target table
            table_mapping_df = mapping_file_df[mapping_file_df['Target Table Name'] == target_table_name]

            # Get all columns where 'Order By' columns is not null and sort by 'Order By' column from table_mappings as a sorted list of columns
            sort_columns_list = table_mapping_df.loc[table_mapping_df['Order By'].notnull()].sort_values(by='Order By')['Target Column Name'].tolist()
            sort_columns_list = cleanse_column_list(sort_columns_list)

            # Get all columns where 'Partition By' columns is not null and sort by 'Partition By' column from table_mappings
            partition_columns_list = table_mapping_df.loc[table_mapping_df['Partition By'].notnull()].sort_values(by='Partition By')['Target Column Name'].tolist()
            partition_columns_list = cleanse_column_list(partition_columns_list)

            if target_data_area in ('silver', 'gold'):
                sort_columns_list.append("MergeKey")
                target_column_schema_list.append({ "name": "MergeKey", "type": "string" })
            else:
                partition_columns_list.append('IngestDate')

            # Create the target schema header
            target_schema_header = {
                "domain": domain,
                "table_name": target_table_name,
                "sort_columns": sort_columns_list,
                "partition_columns": partition_columns_list
            }

            for _, row in table_mapping_df.iterrows():

                target_column_schema = {}

                # Strip whitespace from the column names
                table_mapping_df.loc[:, "Source Table Name"] = table_mapping_df["Source Table Name"].str.strip()
                table_mapping_df.loc[:, "Source Column Name"] = table_mapping_df["Source Column Name"].str.strip()
                table_mapping_df.loc[:, "Target Table Name"] = table_mapping_df["Target Table Name"].str.strip()
                table_mapping_df.loc[:, "Target Column Name"] = table_mapping_df["Target Column Name"].str.strip()

                target_column_name = cleanse_column_list([row["Target Column Name"]])[0]
                target_column_schema["name"] = target_column_name

                if pd.isna(row["Source Column Name"]):
                    # Create the target column schema
                    target_column_schema["type"] = PANDAS_DATATYPE_MAP.get(row['Data Type'], 'string')
                else:
                    source_column_name = cleanse_column_list([row["Source Column Name"]])[0]

                    # Get the source column schema from the source schema
                    source_column_schema = [col for col in curr_source_schema_dict['columns'] if col['name'] == source_column_name][0]

                    # Create the target column schema
                    target_column_schema["type"] = PANDAS_DATATYPE_MAP.get(source_column_schema['type'], 'string')
                    
                # Add the target column schema to the list of target column schemas
                target_column_schema_list.append(target_column_schema)
            
            # Get audit_columns from mint_data_areas config
            target_area_audit_columns = get_data_area_audit_columns(target_data_area)

            # Add audit_columns to target_column_schema_list
            target_column_schema_list+=target_area_audit_columns

            # Add the target column schema list to the target schema header
            target_schema_header['columns'] = target_column_schema_list

            # Add the target schema header to the target tables schema dictionary
            target_tables_schema_dict[target_table_name] = target_schema_header

        return target_tables_schema_dict

    except Exception as e:
        # Handle the exception, log an error message, and potentially raise or return an error indicator
        error_message = f"Error generating target schema for {target_data_area}: {str(e)}"
        logging.error(error_message)
        raise ValueError(error_message)

def generate_pipeline_config(domain: str, target_data_area: str, mapping_file_df: pd.DataFrame) -> dict:
    pipeline_configurations_dict = {}

    try:

        filtered_df = mapping_file_df[['Source Table Name', 'Target Table Name']].drop_duplicates().dropna()

        for _, row in filtered_df.iterrows():
            source_table_name = cleanse_column_list([row['Source Table Name']])[0]
            target_table_name = cleanse_column_list([row['Target Table Name']])[0]

            pipeline_configurations_dict[target_table_name] = {
                "source_data_area": TARGET_SOURCE_AREA_MAP[target_data_area],
                "source_table_path": "cura",
                "source_table_name": source_table_name,
                "target_data_area": target_data_area,
                "target_table_path": "cura",
                "target_table_name": target_table_name,
            }

        return pipeline_configurations_dict

    except Exception as e:
        # Handle the exception, log an error message, and potentially raise or return an error indicator
        error_message = f"Error generating pipeline configurations: {str(e)}"
        logging.error(error_message)
        raise ValueError(error_message)

def generate_mapping_config(domain: str, target_data_area: str, source_schema_dict: dict, mapping_file_df: pd.DataFrame) -> dict:
    mapping_config_dict = {}

    target_table_list = mapping_file_df['Target Table Name'].unique().tolist()
    target_table_list = cleanse_column_list(target_table_list)

    for target_table_name in target_table_list:

        logging.debug(f">>> Generating mapping configuration for {target_table_name}...")

        mapping_config_header = {
            "domain": domain,
            "table_name": target_table_name,
            "source": TARGET_SOURCE_AREA_MAP[target_data_area],
            "target": target_data_area,
            "columns": []
        }

        # Define a set of affirmative responses
        affirmative_responses = {'y', 'yes', 'true', 't', '1', 'x'}

        # Filter the mapping_file_df for the current target table
        target_table_filter_df = mapping_file_df['Target Table Name'] == target_table_name
        filtered_mapping_df = mapping_file_df[target_table_filter_df]

        if 'Is Business Key' in filtered_mapping_df.columns:
            # Filter the filtered DataFrame for columns where 'Is Business Key' is an affirmative response
            business_key_filter = filtered_mapping_df['Is Business Key'].str.lower().isin(affirmative_responses)
            business_key_columns_list = filtered_mapping_df[business_key_filter]['Target Column Name'].tolist()
            business_key_columns_list = cleanse_column_list(business_key_columns_list)

            merge_key_column = {
                "name": "MergeKey",
                "type": "string",
                "source": {
                    "function": "key_hash",
                    "params": {
                        "column_names": business_key_columns_list
                    },
                    "type": "string"
                }
            }

            mapping_config_header['columns'] = [merge_key_column]

        for _, target_column_row in filtered_mapping_df.iterrows():
            target_column_name = target_column_row['Target Column Name'].strip()
            source_column_name = target_column_row['Source Column Name']
            source_table_name = target_column_row['Source Table Name']

            column_mapping_dict = {
                "name": target_column_name
            }

            source_dict = {}
            target_col_data_type = "string" # Default data type is string

            # Case: If Data Type is specified in the mapping file
            if 'Data Type' in filtered_mapping_df.columns:
                target_col_data_type = target_column_row['Data Type'].strip() if isinstance(target_column_row['Data Type'], str) else target_column_row['Data Type']
                target_col_data_type = PANDAS_DATATYPE_MAP.get(target_col_data_type, 'string')
    
            if isinstance(source_column_name, str) and source_column_name.strip() != "" and isinstance(source_table_name, str) and source_table_name.strip() != "":
                source_column_name = cleanse_column_list([source_column_name])[0]
                source_table_name = cleanse_column_list([source_table_name])[0]

                # lookup source data type in source_schema_dict for source_column_name
                source_schema_data_type = [col['type'] for col in source_schema_dict.get(source_table_name, {}).get('columns', []) if col['name'] == source_column_name][0]
                target_col_data_type = PANDAS_DATATYPE_MAP.get(source_schema_data_type, 'string')
            
                source_dict = {
                    "name": source_column_name,
                    "type": target_col_data_type
                }
            else:
                if 'Transformation Function' in filtered_mapping_df.columns:
                    try:
                        transformation_func_column = target_column_row['Transformation Function']
                        if isinstance(transformation_func_column, str) and transformation_func_column != "":
                            source_dict = json.loads(transformation_func_column)
                            source_dict["type"] = "string"
                    except json.JSONDecodeError as e:
                        # Log the error and continue processing other columns
                        logging.error(f"Error parsing JSON for row {_}: {target_column_row}")
                        logging.error(f"Error message: {str(e)}")
                        continue  # Continue processing other columns

            column_mapping_dict["type"] = target_col_data_type
            column_mapping_dict["source"] = source_dict

            mapping_config_header['columns'].append(column_mapping_dict)

        mapping_config_dict[target_table_name] = mapping_config_header

        if mapping_config_dict is None or len(mapping_config_dict) == 0:
            logging.error(f"Mapping configuration file for {target_table_name} is empty.")
            raise Exception(f"Mapping configuration file for {target_table_name} is empty.")
        
        # Make sure the mapping_config_dict columns are not empty
        if mapping_config_dict[target_table_name]['columns'] is None or len(mapping_config_dict[target_table_name]['columns']) == 0:
            logging.error(f"Mapping configuration file for {target_table_name} has no columns.")
            raise Exception(f"Mapping configuration file for {target_table_name} has no columns.")

    return mapping_config_dict

def process_mapping_file(mode: str, domain: str, target_data_area: str, mapping_file_df: pd.DataFrame, source_tables_schema_dict: dict) -> dict:
    """
    Process the mapping file to generate the schema and mapping files.

    Parameters:
    - domain: Domain in which files are to be stored.
    - target_data_area: Target data area for the mapping file.
    - mapping_file_df: Pandas DataFrame containing the mapping file.
    - source_tables_schema_dict: Dictionary containing the source tables schema.

    Returns:
    - output_config_files: List of config files generated.
    """
    
    output_config_files = []

    try:
        # Write the target tables config for domain
        logging.info(f"Generating target tables configuration file for {domain} in {target_data_area}...")
        target_tables_dict = generate_target_table_config(target_data_area, mapping_file_df)
        target_tables_config_filename = f"{target_data_area}_target_tables_{domain}.json"
        write_config_file(domain, target_data_area, target_tables_config_filename, target_tables_dict)
        output_config_files.append(target_tables_config_filename)

        # Create a copy of the source_tables_schema_dict
        original_source_tables_schema_dict = copy.deepcopy(source_tables_schema_dict)

        # Write the source schema config for each source table
        logging.info(f"Generating source schema configuration files for {domain} in {target_data_area}...")

        # for each target table in mapping_file_df get the source_table_name and source_schema_dict
        for source_table, source_schema_dict in source_tables_schema_dict.items():
            # Lookup Target Table Name for the source_table
            target_table_name = mapping_file_df[mapping_file_df['Source Table Name'] == source_table]['Target Table Name'].unique().tolist()[0]

            # Overwrite the 'type' value in source_schema_dict with 'string' for each column in source_schema_dict since landing-zone is a CSV file
            if TARGET_SOURCE_AREA_MAP[target_data_area] == 'landing-zone':
                source_schema_dict['columns'] = [{**col, 'type': 'string'} for col in source_schema_dict['columns']]

            logging.debug(f">>> Writing source schema configuration file for {source_table} for {target_table_name}...")
            source_schema_config_filename = f"{TARGET_SOURCE_AREA_MAP[target_data_area]}_schema_{target_table_name}.json"
            write_config_file(domain, target_data_area, source_schema_config_filename, source_schema_dict)
            output_config_files.append(source_schema_config_filename)

        # Write the target schema config for each target table
        logging.info(f"Generating target schema configuration files for {domain} in {target_data_area}...")
        target_schema_config_dict = generate_target_schema_config(domain, target_data_area, original_source_tables_schema_dict, mapping_file_df)
        for target_table_name, target_schema_config in target_schema_config_dict.items():
            logging.debug(f">>> Writing target schema configuration file for {target_table_name}...")
            target_schema_config_filename = f"{target_data_area}_schema_{target_table_name}.json"
            write_config_file(domain, target_data_area, target_schema_config_filename, target_schema_config)
            output_config_files.append(target_schema_config_filename)

        # Write the pipeline configuration files for each target table
        logging.info(f"Generating pipeline configuration files for {domain} in {target_data_area}...")
        pipeline_config_dict = generate_pipeline_config(domain, target_data_area, mapping_file_df)
        for target_table_name, pipeline_config in pipeline_config_dict.items():
            logging.debug(f">>> Writing pipeline configuration file for {target_table_name}...")
            pipeline_config_filename = f"{target_data_area}_pipeline_{target_table_name}.json"
            write_config_file(domain, target_data_area, pipeline_config_filename, pipeline_config)
            output_config_files.append(pipeline_config_filename)

        # Write the mapping configuration files for each target table
        logging.info(f"Generating mapping configuration files for {domain} in {target_data_area}...")
        mapping_config_dict = generate_mapping_config(domain, target_data_area, original_source_tables_schema_dict, mapping_file_df)

        for target_table_name, map_config in mapping_config_dict.items():
            logging.debug(f">>> Writing mapping configuration file for {target_table_name}...")

            if map_config is None or len(map_config) == 0:
                logging.error(f"Mapping configuration file for {target_table_name} is empty.")
                raise Exception(f"Mapping configuration file for {target_table_name} is empty.")

            map_config_filename = f"{target_data_area}_map_{target_table_name}.json"
            write_config_file(domain, target_data_area, map_config_filename, map_config)
            output_config_files.append(map_config_filename)

    except Exception as e:
        # Handle exceptions, log error messages, and potentially raise or return an error indicator
        error_message = f"Error processing mapping file for {domain}/{target_data_area}: {str(e)}"
        logging.error(error_message)
        raise ValueError(error_message)

    return output_config_files
