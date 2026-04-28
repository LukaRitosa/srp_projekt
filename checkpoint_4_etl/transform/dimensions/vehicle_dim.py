from pyspark.sql.functions import col, trim, regexp_extract, initcap, lower
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from spark_session import get_spark_session

def transform_vehicle_dim(vehicle_type_df, vehicle_category_df, csv_vehicle_df=None):
    spark = get_spark_session()

     # Aliases
    v = vehicle_type_df.alias("v")
    vc = vehicle_category_df.alias("vc")

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        v.join(vc, col("v.category_fk") == col("vc.id"), "left")
        .select(
            trim(col("v.name")).alias("vehicle_type"),
            col("v.capacity").alias("capacity"),
            col("v.wheels").alias("wheels"),
            trim(col("vc.name")).alias("category"),
        )
        .withColumn("vehicle_type", initcap(trim(col("vehicle_type"))))
        .withColumn("category", initcap(trim(col("category"))))
        .dropDuplicates(["vehicle_type"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_vehicle_df:
        csv_df = (
            csv_vehicle_df
            .select(
                col("vehicle_type").alias("vehicle_type"),
                col("capacity").alias("capacity"), 
                col("wheels").alias("wheels"), 
                col("category").alias("category")
            )
            .withColumn("vehicle_type", initcap(trim(col("vehicle_type"))))
            .withColumn("category", initcap(trim(col("category"))))
            .select("vehicle_type", "capacity", "wheels", "category")
            .dropDuplicates(["vehicle_type"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["vehicle_type"])

    window = Window.orderBy("vehicle_type")
    combined_df = combined_df.withColumn("vehicle_tk", row_number().over(window))

    print("Vehicle row count:", combined_df.count())
    assert combined_df.count() == 14, "Number of vehicles records from step one of the project."
    return combined_df