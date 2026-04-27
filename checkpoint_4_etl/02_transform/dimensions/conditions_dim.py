from pyspark.sql.functions import col, trim, regexp_extract, initcap
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from spark_session import get_spark_session

def transform_conditions_dim(accident_df, weather_df, road_surface_df, light_df, csv_conditions_df=None):
    spark = get_spark_session()

     # Aliases
    a = accident_df.alias("a")
    w = weather_df.alias("w")
    r = road_surface_df.alias("r")
    l = light_df.alias("l")

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        a.join(w, col("a.weather_conditions_fk") == col("w.id"), "left")
        .join(r, col("a.road_surface_conditions_fk") == col("r.id"), "left")
        .join(l, col("a.light_conditions_fk") == col("l.id"), "left")
        .select(
            trim(col("w.name")).alias("weather"),
            trim(col("r.name")).alias("road_surface"),
            trim(col("l.name")).alias("light"),
        )
        .withColumn("weather", initcap(trim(col("weather"))))
        .withColumn("road_surface", initcap(trim(col("road_surface"))))
        .withColumn("light", initcap(trim(col("light"))))
        .dropDuplicates()
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_conditions_df:
        csv_df = (
            csv_conditions_df
            .select(
                trim(col("weather_conditions")).alias("weather"),
                trim(col("road_surface_conditions")).alias("road_surface"),
                trim(col("light_conditions")).alias("light"),
            )
            .select("weather", "road_surface", "light")
            .dropDuplicates()
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates()

    window = Window.orderBy("weather", "road_surface", "light")
    combined_df = combined_df.withColumn("conditions_tk", row_number().over(window))

    return combined_df