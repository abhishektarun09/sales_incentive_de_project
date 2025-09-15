from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger

# S3 Client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response['Buckets'])