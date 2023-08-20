from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config

from pyspark.sql.functions import current_timestamp

my_conf = SparkConf()
my_conf.set('spark.app.name', 'race_results')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

drivers_df = spark.read.format('parquet').option('path', config.drivers_outpath).load()

driver_fnl_df = drivers_df.withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('name', 'driver_name')\
    .withColumnRenamed('nationality', 'driver_nationality')

constructor_df = spark.read.format('parquet').option('path', config.constructor_outpath).load()

constructor_fnl_df = constructor_df.withColumnRenamed('name', 'team')

circuits_df = spark.read.format('parquet').option('path', config.circuits_outpath).load()

circuits_fnl_df = circuits_df.withColumnRenamed('location', 'circuit_location')

results_df = spark.read.format('parquet').option('path', config.results_outpath).load()

results_fnl_df = results_df.withColumnRenamed('time', 'race_time')

races_df = spark.read.format('parquet').option('path', config.races_outpath).load()

races_fnl_df = races_df.withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('race_timestamp', 'race_date')

race_circuits_df = races_fnl_df.join(circuits_fnl_df, races_fnl_df.circuit_id == circuits_fnl_df.circuit_id, 'inner')\
    .select(races_fnl_df.race_id, races_fnl_df.race_year, races_fnl_df.race_name, races_fnl_df.race_date,
            circuits_fnl_df.circuit_location)

race_results_df = results_fnl_df.join(race_circuits_df, results_fnl_df.race_id == race_circuits_df.race_id, 'inner')\
    .join(driver_fnl_df, results_fnl_df.driver_id == driver_fnl_df.driver_id, 'inner') \
    .join(constructor_fnl_df, results_fnl_df.constructor_id == constructor_fnl_df.constructor_id, 'inner')

final_df = race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 'driver_name',
                                  'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap',
                                  'race_time', 'points', 'position').withColumn('created_date', current_timestamp())

final_df.write.mode('overwrite').parquet(config.final_results_path)