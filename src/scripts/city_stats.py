
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
from typing import List
from datetime import date as dt, timedelta

from scripts.distance import distance


def city_of_the_event(df_city: DataFrame, df_event: DataFrame) -> DataFrame:

    window = Window().partitionBy("user_id").orderBy(F.col("dist_to_city").asc())
    city_list = df_city.selectExpr(["collect_list(id) as city_list"]) \
                       .withColumn("id", F.lit("1"))

    return df_event.withColumn("id", F.lit("1")) \
                   .join(city_list, "id").drop("id") \
            .selectExpr(["*", "explode(city_list) as id"]) \
            .join(df_city, "id", "left").drop("id", "city_list") \
            .transform(lambda df: distance(df, "to_city", "phi_city", "lat", "lam_city", "lon")) \
            .withColumn("zone_id", F.first("city").over(window))

def month_input_paths(base_input_path:str, date_from: dt) -> List[str]:
    paths = []
    date_to = date_from.replace(month=date_from.month + 1)
    depth = date_from - date_to
    for d in range(depth):
        current_date = (date_from + timedelta(days=d)).isoformat()
        paths.append(f"{base_input_path}/date={current_date}/")

    return paths

def week_input_paths(base_input_path:str, date_from: dt) -> List[str]:
    paths = []
    for d in range(7):
        current_date = (date_from + timedelta(days=d)).isoformat()
        paths.append(f"{base_input_path}/date={current_date}/")

    return paths

def subs_count(spark: SparkSession, df_city: DataFrame, month_paths: List[str], week_paths: List[str]) -> DataFrame:

    df_month = spark.read.parquet(*month_paths) \
                      .where("event_type = 'subscription' and event.subscription_user is not null and event.subscription_channel is not null") \
                      .selectExpr(["event.subscription_user as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as month_subscription "))

    df_week = spark.read.parquet(*week_paths) \
                      .where("event_type = 'subscription' and event.subscription_user is not null and event.subscription_channel is not null") \
                      .selectExpr(["event.subscription_user as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as week_subscription "))

    return df_month.join(df_week, "zone_id", "full-outer")

def messages_count(spark: SparkSession, df_city: DataFrame, month_paths: List[str], week_paths: List[str]) -> DataFrame:

    df_month = spark.read.parquet(*month_paths) \
                      .where("event_type = 'message' and event.message_from is not null and event.message_to is not null") \
                      .selectExpr(["event.message_from as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as month_message"))

    df_week = spark.read.parquet(*week_paths) \
                      .where("event_type = 'message' and event.message_from is not null and event.message_to is not null") \
                      .selectExpr(["event.message_from as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as week_message"))

    return df_month.join(df_week, "zone_id", "full-outer")

def reactions_count(spark: SparkSession, df_city: DataFrame, month_paths: List[str], week_paths: List[str]) -> DataFrame:

    df_month = spark.read.parquet(*month_paths) \
                      .where("event_type = 'reaction' and event.reaction_from is not null") \
                      .selectExpr(["event.reaction_from as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as month_reaction"))

    df_week = spark.read.parquet(*week_paths) \
                      .where("event_type = 'reaction' and event.reaction_from is not null") \
                      .selectExpr(["event.message_from as user_id", "lat", "lon"]) \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as week_reaction "))

    return df_month.join(df_week, "zone_id", "full-outer")

def registrations_count(spark: SparkSession, df_city: DataFrame, events_base_path: str, month_date: dt, week_date: dt) -> DataFrame:

    month_date_from = month_date.isoformat()
    month_date_to = month_date.replace(month=month_date.month + 1).isoformat()

    week_date_from = week_date.isoformat()
    week_date_dict = week_date.isocalendar()
    week_date_dict["week"] += 1
    week_date_to = dt.fromisocalendar(week_date_dict["year"], week_date_dict["week"], week_date_dict["day"]).isoformat()

    window_first = Window().partitionBy("user_id").orderBy(F.col("message_ts").asc())

    df_month = spark.read.parquet(events_base_path) \
                      .where(f"date < {month_date_to} and event_type = 'message' and event.message_from is not null and event.message_to is not null") \
                      .selectExpr(["event.message_from as user_id", "event.message_id as message_id", "event.message_ts as message_ts", "lat", "lon"]) \
                      .withColumn("first_msg", F.first("message_id").over(window_first)) \
                      .where(f"first_msg = message_id and message_ts >= {month_date_from}") \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as month_message"))

    df_week = spark.read.parquet(events_base_path) \
                      .where(f"date < {week_date_to} and event_type = 'message' and event.message_from is not null and event.message_to is not null") \
                      .selectExpr(["event.message_from as user_id", "event.message_id as message_id", "event.message_ts as message_ts", "lat", "lon"]) \
                      .withColumn("first_msg", F.first("message_id").over(window_first)) \
                      .where(f"first_msg = message_id and message_ts >= {week_date_from}") \
                      .transform (lambda df: city_of_the_event(df_city, df)) \
                      .groupBy("zone_id").agg(F.expr("count(user_id) as month_message"))

    return df_month.join(df_week, "zone_id", "full-outer")



def __main__():

    month = sys.argv[1]
    week = sys.argv[2]
    events_base_path = sys.argv[3]
    city_base_path = sys.argv[4]
    output_base_path = sys.argv[5]

    spark = SparkSession \
            .builder \
            .master("yarn") \
            .appName("City stats job") \
            .getOrCreate()

    year =  dt.today().year
    month_date_from = dt(year, int(month), 1)
    week_date_from = dt.fromisocalendar(year, int(week), 1)

    month_paths = month_input_paths(events_base_path, month_date_from)
    week_paths = week_input_paths(events_base_path, week_date_from)

    df_city = spark.read.parquet(city_base_path)

    df_city_stats = messages_count(spark, df_city, month_paths, week_paths) \
            .join(reactions_count(spark, df_city, month_paths, week_paths), "zone_id", "full-outer") \
            .join(subs_count(spark, df_city, month_paths, week_paths), "zone_id", "full-outer") \
            .join(registrations_count(spark, df_city, events_base_path, month_date_from, week_date_from), "zone_id", "full-outer") \
            .withColumn("month", F.lit(month)) \
            .withColumn("week", F.lit(week))

    df_city_stats.write.partitionBy([month, week]).mode("overwrite").format("parquet").save(output_base_path)

    spark.stop()


if __name__ == "__main__":

