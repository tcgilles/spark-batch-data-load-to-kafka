import sys
import os
import uuid
from typing import Any
from dotenv import load_dotenv
import pyspark.sql
import pyspark.sql.functions as f
from lib import utils, config_loader
import lib.transformations as tr
from lib.logger import Log4j

load_dotenv()

KAFKA_API_KEY: str = os.environ.get("KAFKA_API_KEY")
KAFKA_API_SECRET: str = os.environ.get("KAFKA_API_SECRET")

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env: str = sys.argv[1].upper()
    load_date: str = sys.argv[2]
    job_run_id: str = f"SBDL-{str(uuid.uuid4())}"

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf: dict[str, Any] = config_loader.get_config(job_run_env)
    hive_db: str = conf["hive.database"]

    print("Creating Spark Session")
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    logger.info("Finished creating Spark Session")

    logger.info("Loading the data")
    logger.info("Loading accounts")
    accounts_df: pyspark.sql.DataFrame = tr.load_dataframe(spark, hive_db,
                                                           tr.get_accounts_schema(),
                                                           "./test_data/accounts/")
    logger.info("Loading addresses")
    address_df: pyspark.sql.DataFrame = tr.load_dataframe(spark, hive_db,
                                                          tr.get_address_schema(),
                                                          "./test_data/party_address/")
    logger.info("Loading parties")
    parties_df: pyspark.sql.DataFrame = tr.load_dataframe(spark, hive_db,
                                                          tr.get_parties_schema(),
                                                          "./test_data/parties/")
    logger.info("Finished loading data")

    logger.info("Transforming the tables")
    logger.info("Transforming accounts")
    cleaned_accounts_df: pyspark.sql.DataFrame = tr.prepare_accounts_df(accounts_df)
    logger.info("Transforming address")
    cleaned_address_df: pyspark.sql.DataFrame = tr.prepare_address_df(address_df)
    logger.info("Transforming parties")
    cleaned_parties_df: pyspark.sql.DataFrame = tr.prepare_parties_df(parties_df)
    logger.info("Joining the 3 tables")
    complete_table_df: pyspark.sql.DataFrame = tr.reconstruct_full_table(
        cleaned_parties_df, cleaned_address_df, cleaned_accounts_df
    )
    logger.info("Formatting table in kafka suitable schema")
    kafka_df: pyspark.sql.DataFrame = tr.build_kafka_table(complete_table_df)
    kafka_kv_df: pyspark.sql.DataFrame = kafka_df.select(
        f.col("payload.contractIdentifier.newValue").alias("key"),
        f.to_json(f.struct("*")).alias("value")
    )
    logger.info("Finished transforming the tables")

    logger.info("Sending data to Kafka")
    kafka_kv_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        .option("topic", conf["kafka.topic"]) \
        .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(KAFKA_API_KEY, KAFKA_API_SECRET)) \
        .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        .save()
    logger.info("Finished sending data to Kafka")
