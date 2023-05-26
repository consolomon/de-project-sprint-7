
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
from datetime import date as dt

from scripts.distance import distance


RADIUS = 6371

def user_groups(df_subs: DataFrame) -> DataFrame:
    
    return df_subs.selectExpr(["event.subscription_user as user_id", "event.subscription_channel as channel_id"]) \
                  .where("user_id is not null and channel_id is not null").distinct()

def disconnected_users(df_message: DataFrame) -> DataFrame:

    df_contacts = df_message.select("from", "to").distinct()

    df_users = df_contacts.union(df_contacts.select("to", "from")) \
                          .withColumnRenamed("from", "user_id") \
                          .withColumnRenamed("to", "co_user_id").distinct()
    
    return df_users.join(df_contacts, [df_contacts['from'] == df_users['user_id'], df_contacts['to'] == df_users['co_user_id']], "left_anti")


def users_with_last_geo(df_message: DataFrame) -> DataFrame:
    
    last_msg_window = Window().partitionBy("from").orderBy(F.col("message_ts").desc())

    return df_message.select("from", "message_ts", "phi_user", "lam_user") \
                     .withColumn("last_phi_user", F.first("phi_user").over(last_msg_window)) \
                     .withColumn("last_lam_user", F.first("lam_user").over(last_msg_window)) \
                     .withColumn("act_time", F.first("message_ts").over(last_msg_window)) \
                     .selectExpr(["from as user_id", "last_phi_user as phi_user", "last_lam_user as lam_user", "act_time"]) \
                     .where("phi_user is not null and lam_user is not null")

def city_of_the_event(df_city: DataFrame, df_offers: DataFrame) -> DataFrame:

    window = Window().partitionBy("user_left").orderBy(F.col("dist_to_city").asc())
    city_list = df_city.selectExpr(["collect_list(id) as city_list"]) \
                       .withColumn("id", F.lit("1"))

    return df_offers.withColumn("id", F.lit("1")) \
                   .join(city_list, "id").drop("id") \
                   .selectExpr(["*", "explode(city_list) as id"]) \
                   .join(df_city, "id", "left").drop("id", "city_list") \
                   .transform(lambda df: distance(df, "to_city", "phi_city", "phi_user", "lam_city", "lam_user")) \
                   .withColumn("zone_id", F.first("dist_to_city").over(window)) \
                   .withColumn("timezone_name", F.first("timezone").over(window)) \
                   .drop("city", "timezone", "phi_city", "lam_city", "dist_to_city") \
                   .withColumn("local_time", F.from_utc_timestamp(
                                F.col("act_time"),F.col("timezone_name")
                   )).drop("act_time", "timezone_name", "phi_user", "lam_user")


def __main__():

    date = sys.argv[1]
    events_base_path = sys.argv[2]
    city_base_path = sys.argv[3]
    output_base_path = sys.argv[4]

    spark = SparkSession \
            .builder \
            .master("yarn") \
            .appName("Friend offers job") \
            .getOrCreate()


    df_subs = spark.read.parquet(events_base_path) \
                    .where(f"event_type = 'subscription' and date <='{date}'") \
                    .transform(lambda df: user_groups(df))

    df_message = spark.read.parquet(events_base_path) \
                  .where("event_type = 'message'") \
                  .selectExpr(["event.message_from as from", "event.message_to as to",
        "event.message_ts as message_ts", "lat as phi_user", "lon as lam_user"])

    df_offers = disconnected_users(df_message)

    df_users_with_geo = users_with_last_geo(df_message)

    df_offers_by_geo = df_offers.join(df_users_with_geo, "user_id", "left") \
                                .join(df_users_with_geo.withColumnRenamed("user_id", "co_user_id") \
                                                       .withColumnRenamed("phi_user", "phi_co_user") \
                                                       .withColumnRenamed("lam_user", "lam_co_user") \
                                                       .withColumnRenamed("act_time", "co_act_time"),
                                        "co_user_id", "left") \
                                .where("phi_user is not null and lam_user is not null and phi_co_user is not null and lam_co_user is not null") \
                                .transform(lambda df: distance(df, "btw_users", "phi_user", "phi_co_user", "lam_user", "lam_co_user")) \
                                .where("dist_btw_users <= 5").select("user_id", "co_user_id", "phi_user", "lam_user", "act_time")
    
    df_city = spark.read.parquet(city_base_path)

    df_friend_offers = df_offers_by_geo.join(df_subs, "user_id", "inner") \
                                       .join(df_subs.withColumnRenamed("user_id", "co_user_id") \
                                                    .withColumnRenamed("channel_id", "co_channel_id"),
                                                "co_user_id", "inner") \
                                       .where("channel_id = co_channel_id") \
                                       .selectExpr(["user_id as user_left", "co_user_id as user_right", "phi_user", "lam_user", "act_time"]) \
                                       .withColumn("processed_dttm", F.lit(dt.today())) \
                                       .transform(lambda df: city_of_the_event(df_city, df))

    df_friend_offers.write.mode("overwrite").parquet(f"{output_base_path}/date={date}")                                   

    spark.stop()
    
    
if __name__ == "__main__":
    __main__() 