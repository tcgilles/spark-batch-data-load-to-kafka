"""Unit tests for the Spark data processing pipeline using pytest and chispa."""
import pytest
from pyspark.sql.types import (StructType, StructField, StringType, NullType,
                               TimestampType, ArrayType, DateType)
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from chispa import assert_df_equality
import lib.transformations as tr
from lib.config_loader import get_config
from lib.utils import get_spark_session

@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Fixture to create a Spark session for testing."""
    return get_spark_session("LOCAL")

@pytest.fixture(scope='session')
def expected_accounts_df(spark: SparkSession) -> DataFrame:
    """Fixture to load the expected accounts DataFrame from JSON."""
    schema = StructType([StructField('account_id', StringType()),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             ArrayType(StructType(
                                                                 [StructField('contractTitleLineType', StringType()),
                                                                  StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())]))])
    return spark.read \
        .format("json") \
        .schema(schema) \
        .load("test_data/results/contract_df.json")

@pytest.fixture(scope='session')
def expected_final_df(spark: SparkSession) -> DataFrame:
    """Fixture to load the expected final DataFrame from JSON."""
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())]))),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    return spark.read \
        .format("json") \
        .schema(schema) \
        .load("test_data/results/final_df.json") \
        .select("keys", "payload")

def test_blank_test(spark: SparkSession):
    """A simple test to ensure the Spark session is created."""
    print(spark.version)
    assert spark.version == "3.5.6"

def test_get_config():
    """Test the configuration loader for different environments."""
    conf_local = get_config("LOCAL")
    conf_qa = get_config("QA")
    assert conf_local["kafka.topic"] == "sbdl_kafka_cloud"
    assert conf_qa["hive.database"] == "sbdl_db_qa"

def test_read_accounts(spark: SparkSession):
    """Test reading the accounts data and filtering active records."""
    accounts_df = tr.load_dataframe(spark, None, tr.get_accounts_schema(),
                                    "./test_data/accounts/") \
        .where("active_ind == 1")
    assert accounts_df.count() == 8

def test_prepare_accounts(spark:SparkSession, expected_accounts_df: DataFrame):
    """Test the transformation of the accounts DataFrame."""
    accounts_df = tr.load_dataframe(spark, None, tr.get_accounts_schema(),
                                    "./test_data/accounts/") \
        .where("active_ind == 1")
    cleaned_accounts_df = tr.prepare_accounts_df(accounts_df)
    assert_df_equality(expected_accounts_df, cleaned_accounts_df, ignore_nullable=True,
                       ignore_row_order=True, ignore_column_order=True)

def test_kafka_kv_df(spark: SparkSession, expected_final_df: DataFrame):
    """Test the final Kafka key-value DataFrame."""
    accounts_df = tr.load_dataframe(spark, None, tr.get_accounts_schema(),
                                    "./test_data/accounts/") \
        .where("active_ind == 1")
    address_df = tr.load_dataframe(spark, None, tr.get_address_schema(),
                                   "./test_data/party_address/")
    parties_df = tr.load_dataframe(spark, None, tr.get_parties_schema(),
                                   "./test_data/parties/")
    cleaned_accounts_df = tr.prepare_accounts_df(accounts_df)
    cleaned_address_df = tr.prepare_address_df(address_df)
    cleaned_parties_df = tr.prepare_parties_df(parties_df)
    complete_table_df = tr.reconstruct_full_table(
        cleaned_parties_df, cleaned_address_df, cleaned_accounts_df
    )
    actual_final_df = tr.build_kafka_table(complete_table_df).select("keys", "payload")
    assert_df_equality(actual_final_df, expected_final_df, ignore_nullable=True,
                       ignore_row_order=True, ignore_column_order=True)
