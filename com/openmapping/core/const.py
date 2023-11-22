
# Constants
OUTPUT_FOLDER = ".\\config\\"

AZURE_KEY_VAULT_URL = "https://{key_vault_name}.vault.azure.net/"
AZURE_SQL_CONNECTION_STRING = "Server=tcp:{server_name}.database.windows.net,1433;Database={database_name};User Id={username};Password={password};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
AZURE_STORAGE_ACCOUNT_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={account_access_key};EndpointSuffix=core.windows.net"

GENERIC_CONNECTION_STRING = "Driver={odbc_driver};Server=<server_name>;Database=<database_name>;Uid=<username>;Pwd=<password>;"
