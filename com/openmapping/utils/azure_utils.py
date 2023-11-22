from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient

from com.openmapping.core.const import AZURE_KEY_VAULT_URL


def get_secrets_from_keyvault(key_vault_name, secret_name) -> str:
    credential = DefaultAzureCredential()
    key_vault_url = AZURE_KEY_VAULT_URL.format(key_vault_name=key_vault_name)
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value if secret and secret.value else None

def upload_local_file_to_adlsg2(storage_account_connection_string:str, container_name:str, file_path:str, blob_name: str):
    service_client = DataLakeServiceClient.from_connection_string(storage_account_connection_string)
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    
    directory_name = "/".join(blob_name.split("/")[:-1])
    if directory_name:
        directory_client = file_system_client.get_directory_client(directory_name)
        try:
            directory_client.create_directory()
        except Exception as e:
            if "ResourceExistsError" not in str(e):
                raise

    file_client = file_system_client.get_file_client(blob_name)
    with open(file_path, 'rb') as f:
        file_contents = f.read()
    file_client.upload_data(file_contents, overwrite=True)


