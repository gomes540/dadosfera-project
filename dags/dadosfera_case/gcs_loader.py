from google.cloud import storage
from google.oauth2 import service_account
import glob
import json


def upload_trips_files_to_gcs(project_id: str, bucket: str, dadosfera_service_account: dict) -> None:
    file_path_list = glob.glob("dags/dadosfera_case/data/*")
    filename_list = [file_pah.split('/')[-1] for file_pah in file_path_list]
    files_to_upload = list(zip(file_path_list, filename_list))
    print(files_to_upload)

    # credentials_dict = {
    # "type": "service_account",
    # "project_id": "felipe-test-332123",
    # "private_key_id": "fd655172626db641be1563ccfebd05387dfd11f2",
    # "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDMhdUMYnxOeRti\nGWX3q1sAUDk7mv/v/B7f7LlNBrfSNACL/nuTaCNLCQvdOFwBofVc0E1h6RPn4/ox\n9f/VxlxT8DrPWJC3i+s9QvuYmPV3bwU/S0S/eS96lyFnsEuCIiWSNquewGdmDnHs\n5h6L2NYRGTgwJJpsxQTAm7W1XICoT/Ryqovmm7fyg+dPhdhqv0qTXaSvrsGL+JaC\nhkccmM078pRX1OoUidBo3AE732129XLtmeLrvdg4gIvWEgYuQbfIHkzRRESdQDdf\nQN6xlxk3/pQ2I2MTBSL4uA2VRuevrNFCiorPiiSE+ooyaizABQmHsnAbptu2YCy+\nNbgnR7J9AgMBAAECggEAQCF/CqEfJWQoR/x60sxyfoipSZ1yv4epDsIl4JdIsKsJ\nuNwVV8WBu5eclsODZieozm+qtBbn3QDJhT0D+b53WQKtDjNRPZysKMuGJPVhkw/G\n72/koL2ZO7GEEgVfx/B5I33tFpxyT7gb1RLbZTKa/UWRLtdrbMvij5rDwZi95wiL\nhkR1rW2luQ9w47b3ZhjhkWInL6iiLNcByIjsy4vaw22uToFIGyV6ktCSo2P57jFQ\npPeBWzf3F5EuE/XyAnOPwdC/7dMQj0+OhXFfJ2mi9qMYKhRnSxMoKS4umPLXGgL9\nA7CtD0yVfztERGizzjU4OkqfO57jxX1WLECs6rXfaQKBgQDxC81qgu4S2jlTHWWD\np+hsdm3/4gxziLD3Y1P8snjMg+SiLqp37c4LPIaUtFPPtHIBdUMRPSSu+rg6zgfU\nOy/mEQcqohmYREQbLxZHG7iu/Vo/4U3L6gWpjcz8X9d+5tmSzxemrP3t6gwardCl\nXIXhmx7GgVAsMZSliWtE+38L8wKBgQDZNfsnS6s2vP0k6WbTg7n+2QvwnxhnNVGn\njH0xa0m01lhrcaXw8AaRa+COI9p67UB2M0xZzsWUlI63xBjFSnnpKvaF76V5RHp6\n86ZJcVV0EShxXhIXbaH6eOqFmMcVgipiHt3kHD+qPL5n8qlSe60eUCadvYqwwn7L\nbZxq6C4TzwKBgCDcAQayDo5XXVUtPrdx3kda5afqQtRFIAq5aaubEMigejx5rBdp\nPZtehuIiqwI3kQsN1zS0ZxnLZ+3sRDj0UJVGYPm421BcjgQ+qQTMjKKeOv1WU2qm\n3lq8z+Lfldrg0Wwn+wtnrb4PF9NTOowwTrfwk4NwAWPk5mSilRL0Td9tAoGAIiCq\nImVwNDyDZZ5KaqdCvPjrOFY25lhpSPL45J/fx8r5v3/uu6lqzsRtVsfpVvEZ9Lhg\noaesQYkJ13O3FKB8ARef/jFGBYSt3c2Ubeuhqofbm9xU6VtvXMRMzMrRVN7Lu/jv\nkhyiICRFIl3tHqmR7LZZKuWR+e3FMD87mUx7mN8CgYEA7YNW/cYkFveZmn7QYQEJ\nK9iX0D/OFPindnTGnrFrNTYYtW7J+gk7KfUtbXoLT5CG0liCRsJpHSduyQraFU8I\nqmGsIoGwIw/TG2R8xRDOLd0Y5Cp1JlC8UucangitZAxQd2UsHdhLF0uLnB2/Nn2L\newrWS5kChWNtOdOD00XSRzk=\n-----END PRIVATE KEY-----\n",
    # "client_email": "dadosfera@felipe-test-332123.iam.gserviceaccount.com",
    # "client_id": "114557913977996792814",
    # "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    # "token_uri": "https://oauth2.googleapis.com/token",
    # "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    # "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dadosfera%40felipe-test-332123.iam.gserviceaccount.com"
    # }
    
    credentials_dict = json.loads(dadosfera_service_account)
    
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(credentials=credentials, project=project_id)
    bucket = client.bucket(bucket)
    for file_path_filename in files_to_upload:
        blob = bucket.blob(f'trips/{file_path_filename[1]}', chunk_size=10485760)
        blob.upload_from_filename(file_path_filename[0])