"""Configuration loader for different environments and Spark settings."""
from typing import Any
import configparser
import pyspark
from pyspark import SparkConf

def get_config(env: str) -> dict[str, Any]:
    """
    Load configuration settings for the specified environment.
    Args:
        env (str): The environment name (e.g., 'LOCAL', 'QA', 'PROD').
    Returns:
        dict[str, Any]: A dictionary containing configuration key-value pairs.
    """
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf: dict[str, Any]  = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf

def get_spark_conf(env: str) -> pyspark.SparkConf:
    """
    Load Spark configuration settings for the specified environment.
    Args:
        env (str): The environment name (e.g., 'LOCAL', 'QA', 'PROD').
    Returns:
        pyspark.SparkConf: A SparkConf object with the loaded settings.
    """
    spark_conf: pyspark.SparkConf = SparkConf()
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("conf/spark.conf")
    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf
