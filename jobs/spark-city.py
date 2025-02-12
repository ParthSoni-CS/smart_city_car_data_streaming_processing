from pyspark.sql import SparkSession
from config import configurations
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql.dataframe import withWatermark
def main():
    spark = SparkSession.builder.appName("SmartCityCarStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0," 
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configurations.get('AWS_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key", configurations.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    # Adjust the log level to minimize the console output on executors 
    spark.sparkContext.setLogLevel('WARN')

    #vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True), 
        StructField("deviceId", StringType(), True), 
        StructField("timestamp", TimestampType(), True), 
        StructField("location", StringType(), True), 
        StructField("speed", DoubleType(), True), 
        StructField("direction", StringType(), True), 
        StructField("make", StringType(), True),
        StructField("model", StringType(), True), 
        StructField("year", IntegerType(), True),  
        StructField("fuelType", StringType(), True), 
    ])

    #gps schema
    gpsSchema = StructType([
        StructField("id", StringType(), True), 
        StructField("deviceId", StringType(), True), 
        StructField("timestamp", TimestampType(), True), 
        StructField("speed", DoubleType(), True), 
        StructField("direction", StringType(), True), 
        StructField("vehcileType", StringType(), True),
    ])

    # traffic schema
    trafficSchema = StructType([
        StructField("id", StringType(), True), 
        StructField("deviceId", StringType(), True), 
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),  
        StructField("timestamp", TimestampType(), True), 
        StructField("snapshot", StringType(), True),
    ])

    # weather schema
    weatherSchema = StructType([
        StructField("id", StringType(), True), 
        StructField("deviceId", StringType(), True), 
        StructField("location", StringType(), True), 
        StructField("timestamp", TimestampType(), True), 
        StructField("temperature", DoubleType(), True), 
        StructField("weatherCondition", StringType(), True), 
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True), 
        StructField("humidity", IntegerType(), True),  
        StructField("airQualityIndex", DoubleType(), True), 
    ])

    # emergency schema
    emergencySchema = StructType([
        StructField("id", StringType(), True), 
        StructField("deviceId", StringType(), True), 
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),   
        StructField("status", StringType(), True), 
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic_name, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic_name)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )
    
    def streamWriter(input, checkpointFolder, output):
        return(input.writeStream
               .format('parquet')
               .option('path', output)
               .outputMode('append')
               .start())
    
    vehicleDf = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDf = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDf = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDf = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDf = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    query1 = streamWriter(vehicleDf, 's3a://spark-streaming-data-smart-city-car/checkpoints/vehicle_data', 
                 's3a://spark-streaming-data-smart-city-car/data/vehicle_data')
    
    query2 = streamWriter(gpsDf, 's3a://spark-streaming-data-smart-city-car/checkpoints/gps_data', 
                 's3a://spark-streaming-data-smart-city-car/data/gps_data')
    
    query3 = streamWriter(trafficDf, 's3a://spark-streaming-data-smart-city-car/checkpoints/traffic_data', 
                 's3a://spark-streaming-data-smart-city-car/data/traffic_data')
    
    query4 = streamWriter(weatherDf, 's3a://spark-streaming-data-smart-city-car/checkpoints/weather_data', 
                 's3a://spark-streaming-data-smart-city-car/data/weather_data')
    
    query5 = streamWriter(emergencyDf, 's3a://spark-streaming-data-smart-city-car/checkpoints/emergency_data', 
                 's3a://spark-streaming-data-smart-city-car/data/emergency_data')
    
    query5.awaitTermination()

if __name__ == "__main__":
    main()