import subprocess
from pymongo import MongoClient
import urllib.parse
import yaml
import check_student_attendance_query


def copy_file_from_minio(mc_alias, bucket_name, db_name, date_str):
    day = date_str[:2]
    month = date_str[2:4]
    year = date_str[-4:]

    minio_object_path =f"{bucket_name}/{year}/{month}/{day}/{db_name}_{year}-{month}-{day}.gz"

    copy_command = [
        "mc", "cp",
        f"{mc_alias}/{minio_object_path}",
        "."
    ]
    try:
        subprocess.run(copy_command, check=True)
        print(f"File copied from MinIO to current directory.")

        tar_command = ["tar", "-xvf", f"{db_name}_{year}-{month}-{day}.gz"]

        subprocess.run(tar_command, check=True)
        print("File extracted to current path.")

    except subprocess.CalledProcessError as e:
        print(f"Failed to process file: {e}")


def drop_database(db_name,collection_name,from_db,date_str, mongo_uri):
    client = MongoClient(mongo_uri)
    db=client[db_name]
    db[collection_name].rename(collection_name+"_"+from_db+"_"+date_str)
    print(f"Database {db_name}  -> {collection_name}_{from_db}_{date_str} renamed successfully.")
    client.close()


def restore_database(db_name, collection_name,gz_file_path, mongo_uri):
    parsed_uri = urllib.parse.urlparse(mongo_uri)
    host_port = mongo_uri.split("@")[1].split('/')[0]
    netloc = parsed_uri.netloc
    if '@' in netloc:
        credentials, host_port = netloc.split('@', 1)
        username, password = credentials.split(':', 1)
    else:
        username, password = None, None


    username = urllib.parse.unquote(username)
    password = urllib.parse.unquote(password)


    command = [
        "mongorestore",
        "--gzip",
        "--host",host_port,
        "--db", db_name,
        "--username",username,
        "--password",password,
        "--collection",collection_name,
        gz_file_path
    ]


    subprocess.run(command, check=True)
    print(f"Database '{db_name}' restored successfully from '{gz_file_path}'.")

def removedbfromfilesfromserver(dbname):
    directory = "/home/netzary/backupscript/"
    pattern = f"{directory}{dbname}*"

    command = f"rm -r {pattern}"
    try:
        print(f"Running command: {command}")
        subprocess.run(command, shell=True, check=True)
        print(f"Successfully deleted files and directories matching {dbname}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def readMultiTenantFile(mc_alias,bucket_name,to_db,date_str,mongo_uri,collection_name):
    stream = open("multi-tenancy.yml", "r")
    docs = yaml.load_all(stream, yaml.FullLoader)
    day = date_str[:2]
    month = date_str[2:4]
    year_last_digit = date_str[-4:]
    for doc in docs:
         for d in doc['tenantDataList']:
             from_db='{}'.format(d['uri'].split(',')[2].split('/')[1].split('?')[0])
             print(from_db)
             gz_file_path = f"/home/netzary/backupscript/{from_db}_{year_last_digit}-{month}-{day}/{from_db}/dhi_student_attendance.bson.gz"
             copy_file_from_minio(mc_alias, bucket_name, from_db, date_str)

             drop_database(to_db,collection_name,from_db,date_str,mongo_uri)

             restore_database(to_db,collection_name, gz_file_path, mongo_uri)

             check_student_attendance_query.restore_attendance_data(d,mongo_uri,date_str)

             removedbfromfilesfromserver(from_db)

if __name__ == "__main__":
    to_db = "dhi_tempqa_college3"
    date_strs = ["18082024","19082024","20082024","21082024"]
    collection_name ="dhi_student_attendance"
    mongo_uri = "mongodb://tempqcollege3:tmpclg%40357@101.53.134.204:47118/dhi_tempqa_college3"
    mc_alias = "heraizenbackup"
    bucket_name = "heraizenbackup"
    for date_str in date_strs:
        readMultiTenantFile(mc_alias, bucket_name, to_db, date_str,mongo_uri,collection_name)