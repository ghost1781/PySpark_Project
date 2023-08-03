from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from pyspark.sql.functions import current_timestamp, expr, to_timestamp, col

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_circuits_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

races_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                  StructField('year', IntegerType(), True),
                                  StructField('round', StringType(), True),
                                  StructField('circuitId', IntegerType(), True),
                                  StructField('name', StringType(), True),
                                  StructField('date', DateType(), True),
                                  StructField('time', StringType(), True),
                                  StructField('url', StringType(), True)])

races_df = spark.read.format('csv').\
    schema(schema=races_schema).\
    option('header', True).\
    option('path', config.races_path).\
    load()

races_with_timestamp_df = races_df.withColumn('ingestion_dt', current_timestamp()).\
    withColumn('race_timestamp', to_timestamp(expr('date||" "||time'), 'yyyy-MM-dd HH:mm:ss'))

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),
                                                   col('year').alias('race_year'), col('round'),
                                                   col('circuitId').alias('circuit_id'), col('name'),
                                                   col('ingestion_dt'), col('race_timestamp'))

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(config.races_outpath)

spark.stop()