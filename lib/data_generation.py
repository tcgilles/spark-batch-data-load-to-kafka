from random import choice, choices
import pyspark.sql
from faker import Faker

fake = Faker()
Faker.seed(0)

ACCOUNT_ID_OFFSET: int = 6982391059
PART_ID_OFFSET: int = 9823462809
SOURCE_SYS: list[str] = ["COR", "COH", "BDL", "ADS", "CML"]
TAX_ID_TYPE: list[str] = ["EIN", "SSP", "CPR"]
COUNTRY: list[str] = ["United States", "Canada", "Mexico"]

def gen_accounts_df(spark: pyspark.sql.SparkSession,
                    load_date: str,
                    num_records: int) -> pyspark.sql.DataFrame:
    branch: list[tuple[str, str]] = [(fake.swift8(), choice(COUNTRY)) for r in range(1, 20)]
    data_list: list[tuple] = [(
        load_date,
        choices([0,1], cum_weights=[10, 90], k=1)[0],
        ACCOUNT_ID_OFFSET + i,
        choice(SOURCE_SYS),
        fake.date_time_between(start_date='-5y', end_date='-3y'),
        choice([fake.company(), fake.name()]),
        choice([fake.name(), None]),
        choice(TAX_ID_TYPE),
        fake.bban()
        ) + (choice(branch))
        for i in range(num_records)
    ]
    return spark.createDataFrame(data_list) \
        .toDF("load_date", "active_ind", "account_id", "source_sys",
              "account_start_date", "legal_title_1", "legal_title_2",
              "tax_id_type", "tax_id", "branch_code", "country")


def gen_account_party(spark: pyspark.sql.SparkSession,
                      load_date: str,
                      num_records: int) -> pyspark.sql.DataFrame:
    data_list_f: list[tuple] = [(
        load_date,
        ACCOUNT_ID_OFFSET + i,
        PART_ID_OFFSET + i,
        "F-N",
        fake.date_time_between(start_date='-5y', end_date='-3y')
    ) for i in range(num_records)]

    data_list_s: list[tuple] = [(
        load_date,
        ACCOUNT_ID_OFFSET + fake.pyint(1, num_records),
        PART_ID_OFFSET + num_records + i,
        "F-S",
        fake.date_time_between(start_date='-5y', end_date='-3y')
    ) for i in range(num_records // 3)]

    return spark.createDataFrame(data_list_f + data_list_s) \
        .toDF("load_date", "account_id", "party_id",
              "relation_type", "relation_start_date")


def gen_party_address(spark: pyspark.sql.SparkSession,
                      load_date: str,
                      num_records: int) -> pyspark.sql.DataFrame:
    data_list_f: list[tuple] = [(
        load_date,
        PART_ID_OFFSET + i,
        fake.building_number() + " " + fake.street_name(),
        fake.street_address(),
        fake.city(),
        fake.postcode(),
        choice(COUNTRY),
        fake.date_between(start_date='-5y', end_date='-3y')
    ) for i in range(num_records)]

    return spark.createDataFrame(data_list_f) \
        .toDF("load_date", "party_id", "address_line_1", "address_line_2",
              "city", "postal_code", "country_of_address", "address_start_date")


def create_data_files(spark: pyspark.sql.SparkSession,
                      load_date: str,
                      num_records: int) -> None:
    accounts_df: pyspark.sql.DataFrame = gen_accounts_df(spark, load_date, num_records)
    accounts_df.coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("test_data/accounts")

    party_df: pyspark.sql.DataFrame = gen_account_party(spark, load_date, num_records)
    party_df.coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("test_data/parties")

    address_df: pyspark.sql.DataFrame = gen_party_address(spark, load_date, num_records)
    address_df.coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("test_data/party_address")
