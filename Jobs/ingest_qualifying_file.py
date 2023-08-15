from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_qualifying_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

qualifying_schema = StructType(fields=[StructField('qualifyId', IntegerType(), False),
                                       StructField('raceId', IntegerType(), True),
                                       StructField('driverId', IntegerType(), True),
                                       StructField('constructorId', IntegerType(), True),
                                       StructField('number', IntegerType(), True),
                                       StructField('position', IntegerType(), True),
                                       StructField('q1', StringType(), True),
                                       StructField('q2', StringType(), True),
                                       StructField('q3', StringType(), True)])

qualifying_df = spark.read.format('json')\
    .schema(qualifying_schema)\
    .option('multiline', True)\
    .option('path', config.qualifying_path).load()

qualifying_fnl_df = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumn('ingestion_dt', current_timestamp())

qualifying_fnl_df.write.parquet(config.qualifying_outpath)