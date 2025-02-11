from pyspark.sql import SparkSession
from config import configurations

def main():
    spark = SparkSession.builder.appName("SmartCityCarStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0," 
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configurations.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configurations.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    # Adjust the log level to minimize the console output on executors 
    spark.sparkContext.setLogLevel('WARN')
    

if __name__ == "__main__":
    main()