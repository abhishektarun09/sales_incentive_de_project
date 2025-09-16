import sys
from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.read.aws_read import S3Reader
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
import os
from src.main.utility.my_sql_session import get_mysql_connection

# S3 Client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response['Buckets'])

# Checks if local directory already has a file
# If file exists, check if the same file is present in the staging area
# with status as A. If so then do not delete and try to run again.
# Else give an error and do not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    statement = f"""
    select distinct file_name
    from ({config.database_name}).({config.product_staging_table})
    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
    """
    logger.info(f"Dynamically created statement: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Last Run Failed! Please check again.")
    else:
        logger.info("No Record Match")
else:
    logger.info("Last run was Successful!")

# Read files from S3    
try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path=folder_path)
    logger.info("Absolute path on S3 Bucket for CSV file %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process")
except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

# Download files from S3
bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info("File path available on S3 under %s bucket and folder name is %s", bucket_name, file_paths)
try:
    downloader = S3FileDownloader(s3_client=s3_client,
                                  bucket_name=bucket_name,
                                  local_directory=local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"File download error: {e}")
    sys.exit()