import argparse
import logging
import pandas as pd

from com.openmapping.core.functions import validate_mapping_file, process_mapping_file

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Main function to generate mapping and schema
def main(input_file, domain, target, file_type, mode):

    output_files = []

    logging.info(f"Validating mapping file {input_file}...")
    mapping_file_df, source_tables_schema_dict  = validate_mapping_file(input_file, domain,  target, file_type)

    if mapping_file_df is None or len(mapping_file_df) == 0:
        raise Exception(f"Mapping file '{input_file}' is empty")
    else:
        logging.info(f"Mapping file '{input_file}' is valid.")

    if source_tables_schema_dict is None or len(source_tables_schema_dict) == 0:
        raise Exception(f"Source tables schema dictionary is empty.")
    else:
        logging.debug(f">> Source tables schema dictionary is valid.")

    logging.info(f"Processing mapping file '{input_file}'...")
    output_files = process_mapping_file(mode, domain, target, mapping_file_df, source_tables_schema_dict)

    return output_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate mapping and schema files.')
    parser.add_argument('-i', '--input', required=True, help='Input Excel or CSV file containing mapping information.')
    parser.add_argument('-d', '--domain', required=True, help='Domain in which files are to be stored.')
    parser.add_argument('-t', '--type', choices=['excel', 'csv'], default='excel', help='Type of the input file (excel/csv).')
    parser.add_argument('-a', '--target', help='Target area for file path landing-zone, bronze, silver, gold, config, log.')
    parser.add_argument('-m', '--mode', choices=['debug','live'], help='When passed this would either prevent or enable uploading files to adlsg2.')
    args = parser.parse_args()
    output_files = main(args.input, args.domain, args.source, args.target, file_type=args.type, mode = args.mode)
    file_list = ', '.join([f"'{file}'" for file in output_files])
    logging.info(f"Generated files ({len(output_files)}): {file_list}")
