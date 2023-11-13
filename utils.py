from pyspark.sql import SparkSession, functions as F
import mysql.connector as cnt

DRIVER = "mysql-connector-j-8.2.0.jar"

HOST = 'localhost'
PORT = '3307'
USER = 'root'
PASSWORD = 'root'


def create_spark_session():
    # Creat Spark Session
    spark = SparkSession.builder \
        .appName("PySpark MySQL Example") \
        .config("spark.driver.extraClassPath", DRIVER) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Create database if not exists
def create_mysql_database(db):
    conn = cnt.connect(
        host = HOST,
        port = PORT,
        user = USER,
        password = PASSWORD
    )
    c = conn.cursor()
    c.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
    conn.close()

    return print(f"Mysql database {db} created!")


# Write data from pyspark dataframe to database
def save_df_to_mysql(df, db, table_name, mode):
    df = df.withColumn("append_date", F.current_timestamp()) if mode == "append" else df

    df.write.format("jdbc") \
    .option("url", f"jdbc:mysql://{HOST}:{PORT}/{db}") \
    .option("dbtable", table_name) \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .mode(mode) \
    .save()

    return print(f"{table_name} table saved in {db} database successfully!") if mode == "append" else None