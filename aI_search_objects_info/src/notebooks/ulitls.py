from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_surrogate_key_from_bussines_key(df: DataFrame,
                                         bussines_key_column_name: str,
                                         surrogate_key_column_name: str) -> DataFrame:

    spark = df.sparkSession

    # Load existing hub table to check if the business key was already processed
    hub_df = spark.table("nleshin_catalog.silver_layer.objects_description_hub")

    # Left join to retrieve existing surrogate keys (if any)
    df = df.alias("src").join(
        hub_df.select(
            F.col(bussines_key_column_name).alias("_hub_bk"),
            F.col(surrogate_key_column_name).alias("_hub_sk"),
        ).alias("hub"),
        on=F.col(f"src.{bussines_key_column_name}") == F.col("hub._hub_bk"),
        how="left",
    )

    # Use existing surrogate key from the hub, otherwise generate one from the business key via hash
    df = df.withColumn(
        surrogate_key_column_name,
        F.when(
            F.col("_hub_sk").isNotNull(),
            F.col("_hub_sk"),
        ).otherwise(
            F.xxhash64(F.col(f"src.{bussines_key_column_name}").cast("string"))
        ),
    )

    # Clean up temporary join columns
    df = df.drop("_hub_bk", "_hub_sk")

    return df