import os

key = ""
iv = ""
salt = ""

#AWS Access And Secret key
aws_access_key = ""
aws_secret_key = ""
bucket_name = "de-project-tarun"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "de_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "D:\\DS_ML\\DE_Project_M_Kumar_FileDownload\\file_from_s3\\"
customer_data_mart_local_file = "D:\\DS_ML\\DE_Project_M_Kumar_FileDownload\\customer_data_mart\\"
sales_team_data_mart_local_file = "D:\\DS_ML\\DE_Project_M_Kumar_FileDownload\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "D:\\DS_ML\\DE_Project_M_Kumar_FileDownload\\sales_partition_data\\"
error_folder_path_local = "D:\\DS_ML\\DE_Project_M_Kumar_FileDownload\\error_files\\"
