PANDAS_DATATYPE_MAP = {
    'smallint': 'integer',
    'decimal': 'double',
    'varchar': 'string',
    'datetime': 'timestamp',
    'int': 'integer',
    'integer': 'integer',
    'bigint': 'long',
    'uniqueidentifier': 'string',
    "date": "timestamp",
    "long" : "long",
    "double" : "double",
    "timestamp" : "timestamp"
}

TARGET_SOURCE_AREA_MAP = {
    'bronze': 'landing-zone',
    'silver': 'bronze',
    'gold': 'silver'
}

MAP_FILE_REQUIRED_COLUMNS_MAP = {
    'landing-zone' : ['Source Table Name', 'Source Column Name', 'Target Table Name', 'Target Column Name'],
    'bronze' : ['Source Table Name', 'Source Column Name', 'Target Table Name', 'Target Column Name', 'Order By', 'Partition By'],
    'bronze' : ['Source Table Name', 'Source Column Name', 'Target Table Name', 'Target Column Name', 'Order By', 'Partition By', 'Is Business Key', 'Data Type', 'Default Value', 'Transformation Function']
}