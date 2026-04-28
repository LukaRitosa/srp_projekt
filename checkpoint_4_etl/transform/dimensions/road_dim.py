from pyspark.sql.functions import col, trim, regexp_extract, initcap
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from spark_session import get_spark_session

def transform_road_dim(accident_df, junction_detail_df, road_df, csv_road_df=None):
    spark = get_spark_session()

     # Aliases
    a = accident_df.alias("a")
    j = junction_detail_df.alias("j")
    r = road_df.alias("r")

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        a.join(j, col("a.junction_detail_fk") == col("j.id"), "left")
        .join(r, col("a.road_type_fk") == col("r.id"), "left")
        .select(
            trim(col("j.name")).alias("junction_detail"),
            trim(col("r.name")).alias("road_type"),
            col("a.speed_limit").alias("speed_limit"),
        )
        .withColumn("junction_detail", initcap(trim(col("junction_detail"))))
        .withColumn("road_type", initcap(trim(col("road_type"))))
        .dropDuplicates(["junction_detail", "road_type", "speed_limit"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_road_df:
        csv_df = (
            csv_road_df
            .select(
                col("junction_detail").alias("junction_detail"),
                col("road_type").alias("road_type"),
                col("speed_limit").alias("speed_limit"),
            )
            .select("junction_detail", "road_type", "speed_limit")
            .dropDuplicates(["junction_detail", "road_type", "speed_limit"])
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["junction_detail", "road_type", "speed_limit"])

    window = Window.orderBy("junction_detail", "road_type", "speed_limit")
    combined_df = combined_df.withColumn("road_tk", row_number().over(window))

    return combined_df