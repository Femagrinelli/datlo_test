import boto3
import requests
import zipfile36 as zipfile
import os

def download_zip_files(i):
    url       = f'https://dadosabertos.rfb.gov.br/CNPJ/Empresas{i}.zip'
    FILE_KEY    = f'zipped/Empresas{i}'

    print(f'Iniciando download do arquivo zip empresa {i}')
    response = requests.get(url)
    response.raise_for_status()

    with open(f"tmpFiles/{FILE_KEY}.zip", "wb") as f:
        f.write(response.content)

def unzip_files(i):
    FILE_KEY    = f'zipped/Empresas{i}'
    zip_file_name = f'tmpFiles/{FILE_KEY}.zip'

    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
        zip_ref.extractall(f'tmpFiles/unzipped/Empresas{i}/')

def upload_to_s3(i):
    S3_BUCKET = 'datlo-companies'
    S3_CLIENT = boto3.client('s3')
    folder_path = f'tmpFiles/unzipped/Empresas{i}'

    files = os.listdir(folder_path)

    for file_name in files:
        new_filename = f'tmpFiles/renamed/empresas{i}.csv'
        os.rename(f'tmpFiles/unzipped/Empresas{i}/'+file_name, new_filename)

        with open(new_filename, 'rb') as csvFile:
            S3_CLIENT.put_object(Bucket=S3_BUCKET, Key=f'companies/empresas{i}.csv', Body=csvFile)


for i in range(0,10):
    download_zip_files(i)
    unzip_files(i)
    upload_to_s3(i)

os.system('sudo rm /home/ubuntu/zipdownloader/tmpFiles/zipped/*')
os.system('sudo rm /home/ubuntu/zipdownloader/tmpFiles/renamed/*')