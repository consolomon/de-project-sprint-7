from datetime import datetime 
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime(2020, 1, 1),
                                }

dag_spark = DAG(
                        dag_id = "sprint-7-project_dag",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_user_address = SparkSubmitOperator(
                        task_id='spark_user_address',
                        dag=dag_spark,
                        application ='/src/scripts/user_address.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [
                            "2022-02-01",
                            "/user/consolomon/data/geo/events",
                            "/user/consolomon/data/geo/city",
                            "/user/consolomon/data/analytics/user_address"
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )

spark_friend_offers = SparkSubmitOperator(
                        task_id='spark_friend_offers',
                        dag=dag_spark,
                        application ='/src/scripts/friend_offers.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [
                            "2022-02-01",
                            "/user/consolomon/data/geo/events",
                            "/user/consolomon/data/geo/city",
                            "/user/consolomon/data/analytics/friend_offers"
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )

spark_city_stats = SparkSubmitOperator(
                        task_id='spark_city_stats',
                        dag=dag_spark,
                        application ='/src/scripts/city_stats.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [
                            "1",
                            "3",
                            "/user/consolomon/data/geo/events",
                            "/user/consolomon/data/geo/city",
                            "/user/consolomon/data/analytics/city_stats"
                        ],
                        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.sql.broadcastTimeout": 1200,
        },
                        executor_cores = 2,
                        executor_memory = '4g',
                        )