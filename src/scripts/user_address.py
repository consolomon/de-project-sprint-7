import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import sys
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Window
import pyspark.sql.functions as F

from datetime import date as dt, timedelta
from scripts.distance import city_of_the_event

SAVE_BASE_PATH = "/user/consolomon/data/analytics/user_address_d28/"
READ_DF_CITY_PATH = "/user/consolomon/data/geo/city"
READ_DF_EVENT_BASE_PATH = "/user/consolomon/data/geo/events"

def user_address(df_event: DataFrame, df_city: DataFrame, date: str, depth: int) -> DataFrame:

    from_date = (dt.fromisoformat(date) - timedelta(days=depth)).isoformat()

    actual_window = Window().partitionBy("user_id").orderBy(F.col("message_ts").desc())
    home_city_window = Window().partitionBy("user_id").orderBy(F.col("count").desc())

    df_event_city = df_event.where(f"date >= '{from_date}' and date <= '{date}'") \
                            .transform(lambda df_event: city_of_the_event(df_city, df_event))
    
    df_travel = df_event_city.withColumn("city_lag", F.lag("city_name").over(actual_window)) \
                             .select("user_id", "city_name", "city_lag").where ("city_name != city_lag") \
                             .groupBy("user_id").agg(
                                 F.expr("collect_list(city_name) as travel_array"),
                                 F.expr("count(city_name) as travel_count"))
    
    return df_event_city.withColumn("act_city", F.first("city_name").over(actual_window)) \
                        .withColumn("act_time", F.first("message_ts").over(actual_window)) \
                        .withColumn("act_timezone", F.first("timezone_name").over(actual_window)) \
                        .groupBy("user_id", "city_name", "act_city", "act_timezone", "act_time").agg(F.expr("count(message_id) as count")) \
                        .withColumn("local_time", F.from_utc_timestamp(
                                F.col("act_time"),F.col("act_timezone")
                        )) \
                        .withColumn("home_city", F.first("city_name").over(home_city_window)) \
                        .drop("city_name", "act_timezone", "count").distinct() \
                        .join(df_travel, "user_id")

def __main__():

    date = sys.argv[1]
    events_base_path = sys.argv[2]
    city_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    spark = SparkSession \
            .builder \
            .master("yarn") \
            .appName("User address job") \
            .getOrCreate()

    df_event = spark.read.parquet(events_base_path) \
                .where("event_type = 'message'") \
                .selectExpr(["event.message_from as user_id", "event.message_id as message_id", "event.message_ts as message_ts", "date", "lat", "lon"])

    df_city = spark.read.parquet(city_base_path)

    df_user_address = user_address(df_event, df_city, date, 28)

    df_user_address.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")   

    spark.stop()


if __name__ == "__main__":
    __main__()  