import logging

def cleanse_column_list(column_list: list) -> list:
    """
    Cleanse the list of columns.

    Parameters:
    - column_list: List of columns to be cleansed.

    Returns:
    - cleansed_column_list: List of cleansed columns.
    """
    cleansed_column_list = []

    try:
        # Remove empty strings from the list of columns
        cleansed_column_list = [col for col in column_list if col != '']

        # Remove nan values from the list of columns
        cleansed_column_list = [col for col in cleansed_column_list if not (isinstance(col, float) and math.isnan(col))]

        # Remove non-breaking spaces from the list of columns
        cleansed_column_list = [col.replace('\u00a0', '').replace('\u200B', '').replace('\u2000', '').replace('\u180E', '') for col in cleansed_column_list]

        # Remove None from the list of columns
        cleansed_column_list = [col for col in cleansed_column_list if col is not None]

        # Strip whitespace from the list of columns
        cleansed_column_list = [col.strip() for col in cleansed_column_list]

        # Deduplicate the list of columns, preserving order
        seen = set()
        cleansed_column_list = [x for x in cleansed_column_list if not (x in seen or seen.add(x))]

    except Exception as e:
        logging.error(f"An error occurred cleansing column list: {e}\ncolumn list: {column_list}\nReturning an empty list...")
        cleansed_column_list = []  # Return an empty list in case of an error

    return cleansed_column_list