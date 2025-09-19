import datetime
import shutil
import sys
import os
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.spark_session import spark_session
from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.main.write.parquet_writer import ParquetWriter

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

if csv_files:
    statement = f"""
    select distinct file_name
    from {config.database_name}.{config.product_staging_table}
    where file_name in ({str(csv_files)[1:-1]}) and status = 'A'
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
'''
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
    sys.exit()'''
    
# List of files in local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at the local directory after download {all_files}")

# Filter only CSV files and create absolute paths

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))
            
    if not csv_files:
        logger.error("No CSV file available to process")
        raise Exception("No CSV file available to process")
else:
    logger.error("No data to process")
    raise Exception("There is no data to process")

logger.info("***************Listing the files***************")
logger.info("List of csv files that needs to be processed %s", csv_files)
logger.info("***************Creating Spark Session***************")

spark = spark_session()

logger.info("***************Spark Session created****************")

# Check schema of CSV files
# If required columns do not match then keep it in lst or error files
# else union all the data into one dataframe

logger.info("***************Checking Schema of data from S3***************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header", "true")\
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns in Schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns are {missing_columns}")
    
    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns for the {data}")
        correct_files.append(data)
        
logger.info(f"List of correct files: {correct_files}")
logger.info(f"List of error files: {error_files}")

# Move error files to error directory
error_folder_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_path, file_name)
            
            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from S3 files download path to '{destination_path}'")
            
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            
            message = move_s3_to_s3(s3_client=s3_client, 
                                    bucket_name=bucket_name, 
                                    source_prefix=source_prefix, 
                                    destination_prefix=destination_prefix, 
                                    file_name=file_name)
            logger.info(f"{message}")
        else:
            logger.info(f"'{file_path}' does not exist.")
else:
    logger.info("*****There are no error files in our dataset*****")

# Staging table must be updated with status Active (A) or Inactive (I)
logger.info(f"***** Updating the product_staging_table that we have started the process *****")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file_path in correct_files:
        file_name = os.path.basename(file_path)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table} "\
            f"(file_name, file_location, created_date, status) "\
            f"Values ('{file_name}', '{file_path}', '{formatted_date}', 'A')"
        insert_statements.append(statements)
    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info("********** Connecting MySQL Server **********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("********** MySQL server connected **********")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
else:
    logger.error("********** There are no files to process **********")
    raise Exception("********** No data available with correct files **********")

logger.info("********** Staging table updated successfully **********")

# Additional columns needs to be taken care of
# Determine additional columns
logger.info("******** Fixing extra columns coming from source ********")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", DoubleType(), True),
    StructField("additional_columns", StringType(), True)
])

logger.info("********** Creating empty dataframe **********")
final_df_to_process = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
#database_client = DatabaseReader(config.url, config.properties)
#final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")

for data in correct_files:
    data_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns: {extra_columns}")
    
    if extra_columns:
        data_df = data_df.withColumn("additional_columns", concat_ws(", ", *extra_columns))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_columns")
        logger.info(f"Processed {data} and added 'additional_columns'")
        
    else:
        data_df = data_df.withColumn("additional_columns", lit(None))\
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_columns")
    
    final_df_to_process = final_df_to_process.unionByName(data_df)
logger.info("***** Final Dataframe before processing *****")
final_df_to_process.show()

# Connect with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Create df of all dim tables in database

# Customer table
customer_df = database_client.create_dataframe(spark, config.customer_table_name)
logger.info("********** Customer table loaded to dataframe **********")
# Product table
product_df = database_client.create_dataframe(spark, config.product_table)
logger.info("********** Product table loaded to dataframe **********")
# Product staging table
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
logger.info("********** Product staging table loaded to dataframe **********")
# Sales team table
sales_team_df = database_client.create_dataframe(spark, config.sales_team_table)
logger.info("********** Sales team table loaded to dataframe **********")
# Stores table
store_df = database_client.create_dataframe(spark, config.store_table)
logger.info("********** Store table loaded to dataframe **********")

s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process,
                                                        customer_df,
                                                        store_df,
                                                        sales_team_df
                                                        )

# Final enriched data
s3_customer_store_sales_df_join.show()

# Write customer data into customer data mart (parquet)
# Write in local then move to S3 for reporting

logger.info("********** Writing data into customer data mart **********")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                .select("ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
                                        "ct.pincode", "phone_number", "sales_date", "total_cost"
                                        )
logger.info("********** Final dataframe for customer data mart **********")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, 
                                config.customer_data_mart_local_file
                                )

logger.info(f"****** Customer data saved in local at {config.customer_data_mart_local_file} ******")

# Move data to customer data mart on S3
logger.info("***** Moving customer data from local To S3 in customer data mart *****")
s3_uploader = UploadToS3(s3_client)
s3_cdm_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory=s3_cdm_directory, 
                                   s3_bucket=bucket_name, 
                                   local_file_path=config.customer_data_mart_local_file
                                   )
logger.info(message)

# Write customer data into sales team data mart (parquet)
# Write in local then move to S3 for reporting

logger.info("********** Writing data into sales team data mart **********")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                .select("store_id", "sales_person_id", "sales_person_first_name", 
                                        "sales_person_last_name", "store_manager_name", "manager_id", 
                                        "is_manager", "sales_person_address", "sales_person_pincode",
                                        "sales_date", "total_cost", 
                                        expr("SUBSTRING(sales_date,1,7) as sales_month")
                                        )
logger.info("********** Final dataframe for sales team data mart **********")
final_sales_team_data_mart_df.show()

parquet_writer.dataframe_writer(final_sales_team_data_mart_df, 
                                config.sales_team_data_mart_local_file
                                )

logger.info(f"****** Sales team data saved in local at {config.sales_team_data_mart_local_file} ******")

# Move data to customer data mart on S3
logger.info("***** Moving Sales team data from local To S3 in sales team data mart *****")
s3_stdm_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory=s3_stdm_directory, 
                                   s3_bucket=bucket_name, 
                                   local_file_path=config.sales_team_data_mart_local_file
                                   )
logger.info(message)