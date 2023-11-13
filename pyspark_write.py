from pyspark.sql import functions as F
from pyspark import SparkFiles
from utils import  create_spark_session, save_df_to_mysql, create_mysql_database

DB = "mydb"
FILENAME = "winemag-data-130k-v2.json"

# Creat Spark Session
spark = create_spark_session()

# Read given file from url
spark.sparkContext.addFile(FILENAME)
df = spark.read.json(SparkFiles.get(FILENAME))
df_filtered = df.filter(F.col("country") == "Italy").select("description", "designation", "points", "price")

# Create mysql database if not exists
db = FILENAME.split('.')[0].replace("-", "_")
create_mysql_database(db)

# Save pyspark DataFrame to mysql database
save_df_to_mysql(df_filtered, DB, "winemag_data", "overwrite")