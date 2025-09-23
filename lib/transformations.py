import pyspark.sql
import pyspark.sql.functions as f

def build_field_struct(df_col: pyspark.sql.Column,
                       col_alias: str) -> pyspark.sql.Column:
    return f.struct(
        f.lit("INSERT").alias("operation"),
        df_col.alias("newValue")
    ).alias(col_alias)

def load_dataframe(spark: pyspark.sql.SparkSession,
                   hive_db: str = None,
                   schema: str = "",
                   filepath: str = ""
                   ) -> pyspark.sql.DataFrame:
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