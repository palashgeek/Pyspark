'''this is module is to read data from sql, transfer data and write to sql sever database'''
from pyspark.sql import SparkSession

CONNECTION_TYPE = "com.microsoft.sqlserver.jdbc.spark"
SQL_USERNAME = "SQLEXPRESS"
SQL_PASSWORD = 'Palash@123'
SQL_DBNAME = 'CallLog'
SQL_SERVERNAME = "Welcome\SQLEXPRESS"

if __name__ == "__main__":
    spark = SparkSession.builder\
.appName("Write to Sql server")\
.config("spark.driver.extraClassPath", "C:/sqljdbc_12.6.1.0_enu/sqljdbc_12.6/enu/mssql-jdbc-12.6.1.jre11.jar") \
.getOrCreate()
    
    
    jdbcDF = spark.read \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .option("url", url) \
    .option("dbtable", table_name) \
    .option("user", username) \
    .option("password", password).load()