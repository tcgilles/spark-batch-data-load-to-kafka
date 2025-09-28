"""Transformation module for processing and transforming data using PySpark."""
import pyspark.sql
import pyspark.sql.functions as f

def get_accounts_schema() -> str:
    """
    Returns the schema for the accounts data as a string.
    """
    return """
        load_date date, active_ind tinyint, account_id string, source_sys string,
        account_start_date timestamp, legal_title_1 string, legal_title_2 string,
        tax_id_type string, tax_id string, branch_code string, country string
    """

def get_address_schema() -> str:
    """
    Returns the schema for the party address data as a string.
    """
    return """
        load_date date, party_id string, address_line_1 string, address_line_2 string,
        city string, postal_code string, country_of_address string, address_start_date date
    """

def get_parties_schema() -> str:
    """
    Returns the schema for the parties data as a string.
    """
    return """
        load_date date, account_id string, party_id string, relation_type string,
        relation_start_date timestamp
    """

def build_field_struct(df_col: pyspark.sql.Column,
                       col_alias: str) -> pyspark.sql.Column:
    """
    Build a struct column with operation, newValue, and oldValue fields.
    Args:
        df_col (pyspark.sql.Column): The column to be wrapped in the struct.
        col_alias (str): The alias for the resulting struct column.
    Returns:
        pyspark.sql.Column: A struct column with the specified alias.
    """
    return f.struct(
        f.lit("INSERT").alias("operation"),
        df_col.alias("newValue"),
        f.lit(None).alias("oldValue")
    ).alias(col_alias)

def load_dataframe(spark: pyspark.sql.SparkSession,
                   hive_db: str = None,
                   schema: str = "",
                   filepath: str = ""
                   ) -> pyspark.sql.DataFrame:
    """
    Load a DataFrame from either a Hive table or a CSV file based on the provided parameters.
    Args:
        spark (pyspark.sql.SparkSession): The Spark session.
        hive_db (str, optional): The Hive database name. If provided, data will be loaded from Hive.
        schema (str): The schema for the CSV file if loading from a file.
        filepath (str): The file path for the CSV file if loading from a file.
    Returns:
        pyspark.sql.DataFrame: The loaded DataFrame.
    """
    if hive_db:
        return spark.sql("select * from {db}.parties",
                         db=hive_db)
    return spark.read \
                .format("csv") \
                .schema(schema) \
                .option("header", "true") \
                .option("sep", ",") \
                .option("mode", "FAILFAST") \
                .load(filepath)

def prepare_accounts_df(account_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Prepare the accounts DataFrame by transforming and structuring its fields.
    Args:
        account_df (pyspark.sql.DataFrame): The input accounts DataFrame.
    Returns:
        pyspark.sql.DataFrame: The transformed accounts DataFrame.
    """
    legal_titles_array: pyspark.sql.Column = f.array_compact(
        f.array(
            f.when(
                f.isnotnull(f.col("legal_title_1")),
                f.struct(
                    f.lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                    f.col("legal_title_1").alias("contractTitleLine")
                )
            ),
            f.when(
                f.isnotnull(f.col("legal_title_2")),
                f.struct(
                    f.lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                    f.col("legal_title_2").alias("contractTitleLine")
                )
            )
        )
    )
    tax_id_struct: pyspark.sql.Column = f.struct(
        f.col("tax_id_type").alias("taxIdType"),
        f.col("tax_id").alias("taxId")
    )
    return account_df.select(
        "account_id",
        build_field_struct(f.col("source_sys"), "sourceSystemIdentifier"),
        build_field_struct(f.col("account_start_date"), "contractStartDateTime"),
        build_field_struct(legal_titles_array, "contractTitle"),
        build_field_struct(tax_id_struct, "taxIdentifier"),
        build_field_struct(f.col("branch_code"), "contractBranchCode"),
        build_field_struct(f.col("country"), "contractCountry")
    )

def prepare_address_df(address_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Prepare the party address DataFrame by transforming and structuring its fields.
    Args:
        address_df (pyspark.sql.DataFrame): The input party address DataFrame.
    Returns:
        pyspark.sql.DataFrame: The transformed party address DataFrame.
    """
    address_struct: pyspark.sql.Column = f.struct(
        f.col("address_line_1").alias("addressLine1"),
        f.col("address_line_2").alias("addressLine2"),
        f.col("city").alias("addressCity"),
        f.col("postal_code").alias("addressPostalCode"),
        f.col("country_of_address").alias("addressCountry"),
        f.col("address_start_date").alias("addressStartDate"),
    )
    return address_df.select(
        "party_id",
        build_field_struct(address_struct, "partyAddress"),
    )

def prepare_parties_df(party_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Prepare the parties DataFrame by transforming and structuring its fields.
    Args:
        party_df (pyspark.sql.DataFrame): The input parties DataFrame.
    Returns:
        pyspark.sql.DataFrame: The transformed parties DataFrame.
    """
    return party_df.select(
        "account_id",
        "party_id",
        build_field_struct(f.col("relation_type"), "partyRelationshipType"),
        build_field_struct(f.col("relation_start_date"), "partyRelationStartDateTime"),
    )

def reconstruct_full_table(party_df: pyspark.sql.DataFrame,
                           address_df: pyspark.sql.DataFrame,
                           account_df: pyspark.sql.DataFrame
                           ) -> pyspark.sql.DataFrame:
    """
    Reconstruct the full table by joining parties, addresses, and accounts DataFrames.
    Args:
        party_df (pyspark.sql.DataFrame): The parties DataFrame.
        address_df (pyspark.sql.DataFrame): The party address DataFrame.
        account_df (pyspark.sql.DataFrame): The accounts DataFrame.
    Returns:
        pyspark.sql.DataFrame: The reconstructed full DataFrame.
    """
    party_address_df: pyspark.sql.DataFrame = party_df.join(
        f.broadcast(address_df),
        "party_id",
        "left"
    ).select(
        "*",
        build_field_struct(f.col("party_id"), "partyIdentifier")
    )
    party_address_agg_df: pyspark.sql.DataFrame = party_address_df \
        .groupBy("account_id") \
        .agg(
            f.array_agg(
                f.struct(
                    "partyIdentifier",
                    "partyRelationshipType",
                    "partyRelationStartDateTime",
                    "partyAddress"
                )
            ).alias("partyRelations")
        )
    return party_address_agg_df.join(
        f.broadcast(account_df),
        "account_id",
        "right"
    )

def build_kafka_table(full_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Build the Kafka table by structuring the full DataFrame into the required format.
    Args:
        full_df (pyspark.sql.DataFrame): The full DataFrame to be transformed.
    Returns:
        pyspark.sql.DataFrame: The transformed DataFrame suitable for Kafka.
    """
    event_header_struct: pyspark.sql.Column = f.struct(
        f.expr("uuid()").alias("eventIdentifier"),
        f.lit("SBDL-Contract").alias("eventType"),
        f.lit(1).alias("majorSchemaVersion"),
        f.lit(0).alias("minorSchemaVersion"),
        f.expr("""date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssxxxx")""").alias("eventDateTime")
    ).alias("eventHeader")

    keys_array: pyspark.sql.Column = f.array(
        f.struct(
            f.lit("contractIdentifier").alias("keyField"),
            f.col("account_id").alias("keyValue"),
        )
    ).alias("keys")

    payload_struct: pyspark.sql.Column = f.struct(
        build_field_struct(f.col("account_id"), "contractIdentifier"),
        "sourceSystemIdentifier",
        "contractStartDateTime",
        "contractTitle",
        "taxIdentifier",
        "contractBranchCode",
        "contractCountry",
        "partyRelations",
    ).alias("payload")

    return full_df.select(
        event_header_struct,
        keys_array,
        payload_struct
    )
