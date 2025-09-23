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