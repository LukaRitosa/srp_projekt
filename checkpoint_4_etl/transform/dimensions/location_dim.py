import requests
import json
from pyspark.sql import Row
from pyspark.sql.functions import col, trim, initcap, lit, when, isnull, row_number, coalesce, current_timestamp, round
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
from spark_session import get_spark_session


def transform_location_dim(accident_df, local_authority_df, police_force_df, country_df, csv_location_df=None):
    spark = get_spark_session()
        
    # Aliases
    a = accident_df.alias("a")
    la = local_authority_df.alias("la")
    pf = police_force_df.alias("pf")
    c = country_df.alias("c")

    # --- Step 1: Normalize MySQL data ---
    merged_df = (
        a.join(la, col("a.local_authority_fk") == col("la.id"), "left")
        .join(pf, col("la.police_force_fk") == col("pf.id"), "left")
        .join(c, col("pf.country_fk") == col("c.id"), "left")
        .select(
            col("a.latitude").alias("latitude"),
            col("a.longitude").alias("longitude"),
            col("a.urban_or_rural_area").alias("urban_or_rural_area"),
            trim(col("la.name")).alias("local_authority"),
            trim(col("pf.name")).alias("police_force"),
            col("c.id").alias("country_id"),
            trim(col("c.name")).alias("country"),
            col("c.population").alias("population"),
        )
        .withColumn("latitude", col("latitude").cast("decimal(10,5)"))
        .withColumn("longitude", col("longitude").cast("decimal(10,5)"))
        .withColumn("urban_or_rural_area", initcap(trim(col("urban_or_rural_area"))))
        .withColumn("local_authority", initcap(trim(col("local_authority"))))
        .withColumn("police_force", initcap(trim(col("police_force"))))
        .withColumn("country", initcap(trim(col("country"))))
        .dropDuplicates(["latitude", "longitude"])
    )

    # --- Step 2: Normalize CSV data ---
    if csv_location_df:
        csv_df = (
            csv_location_df
            .withColumn("urban_or_rural_area", initcap(trim(col("urban_or_rural_area"))))
            .withColumn("local_authority", initcap(trim(col("local_authority"))))
            .withColumn("police_force", initcap(trim(col("police_force"))))
            .withColumn("country", initcap(trim(col("country"))))
            .withColumn("latitude", col("latitude").cast("decimal(10,5)"))
            .withColumn("longitude", col("longitude").cast("decimal(10,5)"))
            .dropDuplicates(["latitude", "longitude"])
        )
        
        # csv_df = csv_df.withColumn("country_id", lit(None).cast("long"))

        country_lookup = (
            country_df.select(col("id").alias("country_id"), initcap(trim(col("name"))).alias("country"))
        )

        csv_df = (
            csv_df.join(country_lookup, on="country", how="left")
        )

        target_cols = [
            "latitude",
            "longitude",
            "urban_or_rural_area",
            "local_authority",
            "police_force",
            "country_id",
            "country",
            "population"
        ]

        csv_df = csv_df.select(target_cols)

        merged_df = merged_df.select("latitude", "longitude", "urban_or_rural_area", "local_authority", "police_force", "country_id", "country", "population") \
                             .unionByName(csv_df) \
                             .dropDuplicates(["latitude", "longitude"])
    

    window = Window.orderBy("latitude", "longitude")
    merged_df = merged_df.withColumn("location_tk", row_number().over(window))

    merged_df = (
        merged_df
        .withColumn("is_current", lit(True))
        .withColumn("date_from", lit("2000-01-01").cast("timestamp"))
        .withColumn("date_to", lit(None).cast("timestamp"))
    )


    final_df = merged_df.select(
        "location_tk",
        "latitude", 
        "longitude", 
        "urban_or_rural_area", 
        "local_authority", 
        "police_force", 
        "country_id", 
        "country", 
        "population",
        "is_current",
        "date_from",
        "date_to"
    )

    return final_df