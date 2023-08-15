from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_constructors_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING,url STRING"

constructor_df = spark.read.format('json').\
    schema(constructors_schema).\
    option('path',config.constructors_path).\
    load()

constructor_new_df = constructor_df.drop('url')

constructors_renamed_df = constructor_new_df.withColumnRenamed('constructorId', 'constructor_id').\
    withColumnRenamed('constructorRef', 'constructor_ref').withColumn('ingestion_dt', current_timestamp())

constructors_renamed_df.write.mode('overwrite').parquet(config.constructor_outpath)