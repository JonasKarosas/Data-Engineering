from pyspark.sql import functions as F
from pyspark import SparkFiles
from utils import  create_spark_session, save_df_to_mysql, create_mysql_database

DB = "jonaskarosas"
URL = "/kaggle/input/amazon-uk-products-dataset-2023/amz_uk_processed_data.csv"

# Creat Spark Session
spark = create_spark_session()

# Read given file from url
spark.sparkContext.addFile(URL)
filename = URL.split('/')[-1] # extract file name from url
df = spark.read.csv(SparkFiles.get(filename))

# Create mysql database if not exists
db = filename.split('.')[0].replace("-", "_")
create_mysql_database(db)

# Save pyspark DataFrame to mysql database
save_df_to_mysql(df, DB, "amzon_uk_product_2023", "overwrite")