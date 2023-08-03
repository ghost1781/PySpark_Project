from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from pyspark.sql.functions import current_timestamp, lit

my_conf = SparkConf()
my_conf.set('spark.app.name', 'ingest_circuits_file')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(), False),
                                     StructField('circuitRef', StringType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('location', StringType(), True),
                                     StructField('country', StringType(), True),
                                     StructField('lat', DoubleType(), True),
                                     StructField('lng', DoubleType(), True),
                                     StructField('alt', IntegerType(), True),
                                     StructField('url', StringType(), True)])

circuits_file = spark.read.format('csv').\
    schema(schema=circuits_schema).\
    option('header', True).\
    option('path', config.circuits_path).\
    load()

circuits_new_df = circuits_file.select('circuitId', 'circuitRef', 'name',
                                       'location', 'country', 'lat', 'lng', 'alt')

circuits_renamed_df = circuits_new_df.withColumnRenamed('circuitId', 'circuit_id').\
    withColumnRenamed('circuitRef', 'circuit_ref')

circuit_final_df = circuits_renamed_df.withColumn('ingestion_dt', current_timestamp())\
    .withColumn('data', lit('Sample'))

''' When you want to add a default value use lit() '''

circuit_final_df.write.mode('overwrite').parquet(path=config.circuits_outpath)

spark.stop()
