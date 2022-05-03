from google.cloud import storage
from google.oauth2 import service_account
import glob
import json


def upload_trips_files_to_gcs(project_id: str, bucket: str, dadosfera_service_account: dict) -> None:
    file_path_list = glob.glob("dags/dadosfera_case/data/trips/*")
    filename_list = [file_pah.split('/')[-1] for file_pah in file_path_list]
    files_to_upload = list(zip(file_path_list, filename_list))
    print(files_to_upload)
    
    credentials_dict = json.loads(dadosfera_service_account)
    
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket)
    for file_path_filename in files_to_upload:
        blob = bucket.blob(f'trips/{file_path_filename[1]}', chunk_size=10485760)
        blob.upload_from_filename(file_path_filename[0])