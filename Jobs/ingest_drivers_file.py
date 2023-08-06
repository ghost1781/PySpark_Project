from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from pyspark.sql.functions import current_timestamp, expr

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_circuits_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

name_schema = StructType(fields=[StructField('forename', StringType(), True),
                                 StructField('surname', StringType(), True)])

drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), False),
                                    StructField('driverRef', StringType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('code', StringType(), True),
                                    StructField('name', name_schema),
                                    StructField('dob', DateType(), True),
                                    StructField('nationality', StringType(), True),
                                    StructField('url', StringType(), True)])

drivers_df = spark.read.schema(drivers_schema).format('json').option('path', config.drivers_path).load()

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id').\
    withColumnRenamed('driverRef', 'driver_ref').\
    withColumn('ingestion_dt', current_timestamp()).withColumn('name', expr('name.forename||" "||name.surname'))

drivers_final_df = drivers_with_columns_df.drop('url')

drivers_final_df.write.mode('overwrite').parquet(config.drivers_outpath)