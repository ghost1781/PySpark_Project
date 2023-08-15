from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_laptimes_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('position', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)])

lap_times_df = spark.read.format('csv') \
    .schema(lap_times_schema) \
    .option('path', config.laptimes_path).load()

lap_times_fnl_df = lap_times_df.withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumn('ingestion_dt', current_timestamp())

lap_times_fnl_df.write.mode("overwrite").parquet(config.laptimes_outpath)