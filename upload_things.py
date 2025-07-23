from os import listdir, environ, makedirs, path
from os.path import isdir, join
from shutil import copy, rmtree
import csv

import boto3

from config import INPUT_FOLDER, OUTPUT_FOLDER
from env_vars import get_env_vars_from_ssm, Environments

SUB_FOLDER_OVERRIDES: dict[str, str] = {
    'Client Void Cheques':  'Client Void Cheques',
    'Client IDs': 'Client IDs'
}

def upload_to_s3(upload_filename: str, filename: str, **kwargs) -> None:
    s3 = boto3.resource(service_name='s3', aws_access_key_id=environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY'], region_name=environ['AWS_S3_REGION_NAME'])
    bucket_name:str = environ['S3_BUCKET']
    with open(filename, 'rb') as file:
        content = file.read()
        s3.Bucket(bucket_name).put_object(Key=upload_filename, Body=content)
    
def s3_bucket_client():
    s3 = boto3.resource(service_name='s3', aws_access_key_id=environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY'], region_name=environ['AWS_S3_REGION_NAME'])
    return s3.Bucket(environ['S3_BUCKET'])

def get_csv(filepath) -> list[str]:
    files = listdir(filepath)
    return [file for file in files if file.endswith('.csv')]

def csv_to_dict_list(csv_file_path) -> list[dict]:
    dict_list = []

    with open(csv_file_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Get the header row
            for row in reader:
                dict_list.append(dict(zip(header, row)))
    return dict_list

def find_dict_with_qid(header_rows: list[str]) -> str|None:
    for key_ in header_rows:
        if 'qid' in key_.lower():
            return key_

def move_files(source:str, destination:str, qid:str):
    makedirs(join(*destination.__str__().split('/')[:-1]), exist_ok=True)
    qid_index = destination.index('.')
    new_destination = f"{destination[:qid_index]}_{qid}{destination[qid_index:]}"
    copy(source, new_destination)
    

def write_tuple_to_csv(file_path, data):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for entry in data:
            writer.writerow(entry)

import os
from os.path import dirname, basename, join


def resolve_case_insensitive_path(filepath: str) -> str | None:
    """
    Given a possibly mis-cased file path, returns the correct-cased full path
    if a matching file exists in the same directory (case-insensitive match).
    """
    folder = os.path.dirname(filepath)
    target = os.path.basename(filepath)

    if not os.path.isdir(folder):
        print(f"Directory does not exist: {folder}")
        return None

    for filename in os.listdir(folder):
        if filename.lower() == target.lower():
            return os.path.join(folder, filename)
    raise FileNotFoundError(f'could not find {folder}/{target}')


def find_case_sensitive_file(filepath: str) -> tuple[str, str]:
    """
    Tries to resolve a case-sensitive filename with possible suffix variations:
    (1) exact match
    (2) adjust suffix case (upper/lower)
    (3) duplicate suffix (e.g. .JPG.jpg or .jpg.jpg)
    
    Returns the resolved full path or None if not found.
    """
    try:
        filepath = resolve_case_insensitive_path(filepath)
    except FileNotFoundError:
        pass

    folder = dirname(filepath)
    base = basename(filepath)

    if not os.path.isdir(folder):
        print(f"Directory does not exist: {folder}")
        return None

    files = os.listdir(folder)

    if base in files:
        return folder, base

    prefix, suffix = base.rsplit('.', 1)
    candidates = [
        f"{prefix}.{suffix.lower()}",
        f"{prefix}.{suffix.upper()}",
        f"{prefix}.{suffix.upper()}.{suffix.lower()}",
        f"{prefix}.{suffix.lower()}.{suffix.lower()}",
    ]

    for candidate in candidates:
        if candidate in files:
            return folder, candidate
    raise AssertionError(f'could not fild {prefix}.{suffix}')


FILENAME = 'filename'

if __name__ == "__main__":
    get_env_vars_from_ssm(Environments.PROD.value)
    
    things_that_were_uploaded: list[str] = [('qid', 'folder', 'sub_folder', 'file_filename', 'upload_name')]
    things_that_were_not_uploaded: list[str] = [('qid', 'folder', 'sub_folder', 'file_filename', 'upload_name')]
    
    for folder in listdir(INPUT_FOLDER):
        if not isdir(join(INPUT_FOLDER, folder)):
            continue
        short_code = folder
                
        for sub_folder in listdir(join(INPUT_FOLDER, folder)):
            if not isdir(join(INPUT_FOLDER, folder, sub_folder)):
                continue
            
            csvs = get_csv(join(INPUT_FOLDER, folder, sub_folder))
            
            assert len(csvs) == 1, f'Wrong number CSVs found. Tell Mike to correct. csvs={csvs} in {join(INPUT_FOLDER, folder, sub_folder)}'
            filenames: list[dict] = csv_to_dict_list(join(INPUT_FOLDER, short_code, sub_folder, csvs[0]))
            assert FILENAME in filenames[0].keys(), f'We require a Filename field in {join(INPUT_FOLDER, short_code, sub_folder, csvs[0])}'
            qid_field = find_dict_with_qid(list(filenames[0].keys()))
            assert qid_field,  'We require a qid in one of the keys'
            
            for file in filenames:
                file_filename:str = file[FILENAME]
                upload_sub_folder: str = SUB_FOLDER_OVERRIDES[sub_folder] if sub_folder in SUB_FOLDER_OVERRIDES else sub_folder
                
                folder_filepath: str = join(INPUT_FOLDER, folder, sub_folder, file_filename)                
                qid = file[qid_field]
                upload_name = f"public/{short_code}/{qid}/Internal/Onboarding/{upload_sub_folder}/{file_filename}"
                
                if qid not in ['None', ""]:
                    local_name = join(INPUT_FOLDER, folder, sub_folder, file_filename)
                    
                    if 'GRYBA.pdf' in file_filename:
                        print(f'skipping {file_filename} ')
                        continue
                    try:
                        folders, file_filename = find_case_sensitive_file(local_name)
                    except AssertionError:
                        print(f'COULD NOT FIND {file_filename}')
                        continue
                    
                    local_name = join(folders, file_filename)
                    
                    try:
                        upload_to_s3(upload_name, local_name)
                        things_that_were_uploaded.append((qid, folder, sub_folder, file_filename, upload_name))
                        try:
                            move_files(folder_filepath, join(OUTPUT_FOLDER, folder, sub_folder, file_filename), qid)
                        except FileNotFoundError:
                            # pass
                            print(f'could not move {join(OUTPUT_FOLDER, folder, sub_folder, file_filename)}')
                    except TimeoutError:
                        things_that_were_not_uploaded.append((qid, folder, sub_folder, file_filename, upload_name))

                else:
                    things_that_were_not_uploaded.append((qid, folder, sub_folder, file_filename, upload_name))
                    move_files(folder_filepath, join(OUTPUT_FOLDER, folder, 'Things that were not uploaded', sub_folder, file_filename))

        write_tuple_to_csv(join(OUTPUT_FOLDER, folder, 'things_that_were_uploaded.csv'), things_that_were_uploaded)
        write_tuple_to_csv(join(OUTPUT_FOLDER, folder, 'things_that_were_not_uploaded.csv'), things_that_were_not_uploaded)

        # rmtree(join(INPUT_FOLDER, folder))
    