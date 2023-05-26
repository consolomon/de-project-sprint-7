import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F


RADIUS = 6371

def distance(df: DataFrame, dist_name: str, phi_1: str, phi_2: str, lam_1: str, lam_2: str) -> DataFrame:
    
    return df.withColumn(f"dlambda_{dist_name}", F.radians(F.col(lam_1).cast("float")) - F.radians(F.col(lam_2).cast("float"))) \
             .withColumn(f"dphi_{dist_name}", F.radians(F.col(phi_1).cast("float")) - F. radians(F.col(phi_2).cast("float"))) \
             .withColumn(f"dist_{dist_name}", F.asin(
                 F.sqrt(
                     F.sin(F.col(f"dphi_{dist_name}") / 2) ** 2
                     + F.cos(F.radians(F.col(phi_1).cast("float")))
                     * F.cos(F.radians(F.col(phi_2).cast("float")))
                     * F.sin(F.col(f"dlambda_{dist_name}") / 2) ** 2
                 )
                 ) * 2 * RADIUS) \
             .drop(f"dlambda_{dist_name}", f"dphi_{dist_name}")


def city_of_the_event(df_city: DataFrame, df_event: DataFrame) -> DataFrame:

    window = Window().partitionBy("message_id").orderBy(F.col("dist_to_city").asc())
    city_list = df_city.selectExpr(["collect_list(id) as city_list"]) \
                       .withColumn("id", F.lit("1"))

    return df_event.withColumn("id", F.lit("1")) \
                   .join(city_list, "id").drop("id") \
            .selectExpr(["*", "explode(city_list) as id"]) \
            .join(df_city, "id", "left").drop("id", "city_list") \
            .transform(lambda df: distance(df, "to_city", "phi_city", "lat", "lam_city", "lon")) \
            .withColumn("city_name", F.first("city").over(window)) \
            .withColumn("timezone_name", F.first("timezone").over(window)) \
            .drop("city", "timezone", "phi_city", "lam_city", "dist_to_city").distinct()

"""
spark = SparkSession \
            .builder \
            .master("yarn") \
            .config("spark.executor.cores", 2) \
            .config("spark.executor.memory", "2g") \
            .appName("Assemble the job") \
            .getOrCreate()

df_event = spark.read.parquet("/user/consolomon/data/geo/events") \
                .where("event_type = 'message'") \
                .selectExpr(["event.message_from as user_id", "event.message_id as message_id", "date", "lat", "lon"])

df_city = spark.read.parquet("/user/consolomon/data/geo/city")

spark.stop()
"""