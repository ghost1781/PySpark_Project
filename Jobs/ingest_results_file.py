from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_results_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('grid', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionText', StringType(), True),
                                    StructField('points', FloatType(), True),
                                    StructField('laps', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('fastestLap', IntegerType(), True),
                                    StructField('rank', IntegerType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('fastestLapSpeed', FloatType(), True),
                                    StructField('statusId', StringType(), True)])

results_df = spark.read.format('json').schema(results_schema).option('path', config.results_path).load()

results_with_columns_df = results_df.withColumnRenamed('resultId', 'result_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('positionText', 'position_text')\
    .withColumnRenamed('positionOrder', 'position_order')\
    .withColumnRenamed('fastestLap', 'fastest_lap')\
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
    .withColumn('ingestion_dt', current_timestamp())

results_final_df = results_with_columns_df.drop('status_id')

results_final_df.write.mode('overwrite').parquet(config.results_outpath)