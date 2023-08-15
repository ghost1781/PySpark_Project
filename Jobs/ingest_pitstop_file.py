from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_pistops_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('stop', StringType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('duration', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)])

pit_stops_df = spark.read.format('json')\
    .schema(pit_stops_schema)\
    .option('multiline', True)\
    .option('path', config.pistops_path).load()

pit_stops_fnl_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumn('ingestion_dt', current_timestamp())

pit_stops_fnl_df.write.mode("overwrite").parquet(config.pitsops_outpath)
