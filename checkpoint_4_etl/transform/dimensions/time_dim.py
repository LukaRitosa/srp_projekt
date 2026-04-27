from pyspark.sql.functions import col, trim, regexp_extract, initcap
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from spark_session import get_spark_session

def transform_time_dim(accident_df, time_df, part_of_day_df, date_df, day_of_week_df, season_df, csv_time_df=None):
    spark = get_spark_session()

     # Aliases
    a = accident_df.alias("a")
    t = time_df.alias("t")
    p = part_of_day_df.alias("p")
    d = date_df.alias("d")
    dw = day_of_week_df.alias("dw")
    s = season_df.alias("s")

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        a.join(t, col("a.accident_time_fk") == col("t.id"), "left")
        .join(p, col("t.part_of_day_fk") == col("p.id"), "left")
        .join(d, col("a.accident_date_fk") == col("d.id"), "left")
        .join(dw, col("d.day_of_week_fk") == col("dw.id"), "left")
        .join(s, col("d.season_fk") == col("s.id"), "left")
        .select(
            col("t.time").alias("time"),
            trim(col("p.name")).alias("part_of_day"),
            col("d.date").alias("date"),
            col("dw.day_of_week").alias("day_of_week"),
            col("s.season").alias("season"),
        )
        .withColumn("part_of_day", initcap(trim(col("part_of_day"))))
        .withColumn("day_of_week", initcap(trim(col("day_of_week"))))
        .withColumn("season", initcap(trim(col("season"))))
        .dropDuplicates()
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_time_df:
        csv_df = (
            csv_time_df
            .select(
                col("time").alias("time"),
                col("part_of_day").alias("part_of_day"), 
                trim(col("accident_date")).alias("date"),
                col("season").alias("season"),
                col("day_of_week").alias("day_of_week")
            )
            .select("time", "part_of_day", "date", "season", "day_of_week")
            .dropDuplicates()
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["time", "date"])

    window = Window.orderBy("time", "date")
    combined_df = combined_df.withColumn("time_tk", row_number().over(window))

    return combined_df.orderBy("time", "date")