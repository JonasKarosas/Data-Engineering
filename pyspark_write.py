from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("PySpark MySQL Example") \
    .config("spark.driver.extraClassPath", "data_engineering_task/mysql-connector-j-8.2.0.jar") \
    .getOrCreate()

jdbc_url = "jdbc:mysql://localhost:3307/mydb"

# Define the DataFrame schema
schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Sample data for the DataFrame
data = [
    (1, "John", "Doe", 30),
    (2, "Jane", "Smith", 28),
    (3, "Bob", "Johnson", 3)
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "test_table") \
    .option("user", "root") \
    .option("password", "root") \
    .mode("overwrite") \
    .save()