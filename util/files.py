from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")


def create_blob_folder(folder_path):
    try:
        # Ensure folder path ends with '/'
        if not folder_path.endswith('/'):
            folder_path += '/'
        
        # Create BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            raise ValueError(f"Container {container_name} doesn't exist")
        
        # Create an empty blob with the folder path as name
        blob_client = container_client.get_blob_client(f"{folder_path}readme.txt")
        blob_client.upload_blob("This is crated via job id xxx", overwrite=True)
        
        print(f"Successfully created folder: {folder_path}")
        return True
        
    except Exception as e:
        print(f"Error creating folder: {str(e)}")
        return False
    
def create_blob_url(path):

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    account_name = blob_service_client.account_name
    credential = blob_service_client.credential.account_key
    # Generate the SAS token
    sas_token = generate_blob_sas(
    account_name=account_name,
    container_name=container_name,
    blob_name=path,
    account_key=credential,
    permission=BlobSasPermissions(write=True),
    expiry=datetime.now(timezone.utc) + timedelta(hours=1)
)

    # Construct the full URL
    blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{path}?{sas_token}"
    # print("Pre-signed URL:", blob_url)
    return blob_url

def create_blob_urls_download(path):

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    account_name = blob_service_client.account_name
    credential = blob_service_client.credential.account_key
    container_client = blob_service_client.get_container_client('jobs')
    

    blob_list = container_client.list_blobs(name_starts_with=path)

    list_of_url = []
    for blob in blob_list:
        # print(blob.name, blob.size)

        if not blob.name.endswith('/') and blob.size > 0 and blob.name.count('/') == path.count('/') + 1:  # Skip directories
            # print(f"Downloading {blob.name}...")
            # print(blob.content_settings.content_type)
            # Generate the SAS token
            sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob.name,
            account_key=credential,
            permission=BlobSasPermissions(read=True),
            protocol='https',
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
            )            

            url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob.name}?{sas_token}"

            list_of_url.append({"blob": blob.name, "file": blob.name.rsplit("/", 1)[-1], "url": url})
            # # Generate presigned URL
            # presigned_url = generate_presigned_url(blob.name)
            
            # # Download the file
            # response = requests.get(presigned_url)
            
            # if response.status_code == 200:
            #     # Create local path (preserve folder structure)
            #     local_path = os.path.join(download_dir, blob.name)
            #     os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
            #     # Save file
            #     with open(local_path, 'wb') as f:
            #         f.write(response.content)
            #     print(f"Successfully downloaded to {local_path}")
            # else:
            #     print(f"Failed to download {blob.name}. Status code: {response.status_code}")



    # # Construct the full URL
    # blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{path}?{sas_token}"
    # print("Pre-signed URL:", blob_url)
    # print(list_of_url)
    return list_of_url

