from pyspark import SparkConf
from pyspark.sql import SparkSession
from conf import config

from pyspark.sql.functions import sum, when, count, col, desc, rank
from pyspark.sql.window import Window

my_conf = SparkConf()
my_conf.set('spark.app.name', 'driver_standings')
my_conf.set('spark.app.master', 'local[*]')

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

race_results_df = spark.read.format('parquet').option('path', config.final_results_path).load()

driver_standings_df = race_results_df.groupby('race_year', 'driver_name', 'driver_nationality', 'team')\
    .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df = driver_standings_df.withColumn('rank', rank().over(driver_rank_spec))

final_df.write.mode('overwrite').parquet(config.driver_standings)