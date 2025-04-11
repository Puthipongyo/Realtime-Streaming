import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Setup logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class':'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users(
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT,
            PRIMARY KEY (username)
        );
    """)
    print("Table created successfully")

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'test_streamming') \
            .option('startingOffsets', 'earliest') \
            .option('failOnDataLoss', 'false') \
            .load()
        logging.info("Kafka dataframe created successfully")
        return spark_df
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}", exc_info=True)
        return None

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('sparkDataStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1," +
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create Spark session due to: {e}", exc_info=True)
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}", exc_info=True)
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        kafka_df = connect_to_kafka(spark_conn)
        if kafka_df is not None:
            selection_df = create_selection_df_from_kafka(kafka_df)
            session = create_cassandra_connection()
            if session is not None:
                create_keyspace(session)
                session.set_keyspace("spark_streams")
                create_table(session)

                logging.info("Starting streaming query...")

                query = selection_df.writeStream \
                    .format("org.apache.spark.sql.cassandra") \
                    .option("checkpointLocation", "/tmp/checkpoint") \
                    .option("keyspace", "spark_streams") \
                    .option("table", "created_users") \
                    .start()

                query.awaitTermination()