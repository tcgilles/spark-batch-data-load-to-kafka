from typing import Any
import configparser

import pyspark
from pyspark import SparkConf

def get_config(env: str) -> dict[str, Any]:
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf: dict[str, Any]  = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf


def get_spark_conf(env: str) -> pyspark.SparkConf:
    spark_conf: pyspark.SparkConf = SparkConf()
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("conf/spark.conf")
    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
