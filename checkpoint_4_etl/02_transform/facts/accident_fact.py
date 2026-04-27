from pyspark.sql.functions import col, trim, initcap, regexp_extract, row_number
from pyspark.sql.window import Window

def transform_accident_fact(
    raw_data,
    dim_conditions_df,
    dim_location_df,
    dim_road_df,
    dim_time_df,
    dim_vehicle_df  
):
    # Extract all raw tables
    accident_df = raw_data["accident"]
    date_df = raw_data["accident_date"]
    day_of_week_df = raw_data["day_of_week"]
    season_df = raw_data["season"]
    part_of_day_df = raw_data["part_of_day"]
    time_df = raw_data["accident_time"]
    weather_df = raw_data["weather_conditions"]
    road_surface_df = raw_data["road_surface_conditions"]
    light_df = raw_data["light_conditions"]
    junction_detail_df = raw_data["junction_detail"]
    road_type_df = raw_data["road_type"]
    vehicle_type_df = raw_data["vehicle_type"]
    vehicle_category_df = raw_data["vehicle_category"]
    local_authority_df = raw_data["local_authority"]
    police_force_df = raw_data["police_force"]
    country_df = raw_data["country"]
    csv_accident_df = raw_data.get("csv_accident")

    # Normalize MySQL sales
    enriched_mysql_accident = (
        accident_df.alias("a")
        .join(date_df.alias("ad"), col("a.accident_date_fk") == col("ad.id"), "left")
        .join(day_of_week_df.alias("dw"), col("ad.day_of_week_fk") == col("dw.id"), "left")
        .join(season_df.alias("s"), col("ad.season_fk") == col("s.id"), "left")
        .join(time_df.alias("at"), col("a.accident_time_fk") == col("at.id"), "left")
        .join(part_of_day_df.alias("pd"), col("at.part_of_day_fk") == col("pd.id"), "left")
        .join(light_df.alias("lc"), col("a.light_conditions_fk") == col("lc.id"), "left")
        .join(weather_df.alias("wc"), col("a.weather_conditions_fk") == col("wc.id"), "left")
        .join(road_surface_df.alias("rsc"), col("a.road_surface_conditions_fk") == col("rsc.id"), "left")
        .join(vehicle_type_df.alias("vt"), col("a.vehicle_type_fk") == col("vt.id"), "left")
        .join(vehicle_category_df.alias("vc"), col("vt.category_fk") == col("vc.id"), "left")
        .join(junction_detail_df.alias("jd"), col("a.junction_detail_fk") == col("jd.id"), "left")
        .join(road_type_df.alias("rt"), col("a.road_type_fk") == col("rt.id"), "left")
        .join(local_authority_df.alias("la"), col("a.local_authority_fk") == col("la.id"), "left")
        .join(police_force_df.alias("pf"), col("la.police_force_fk") == col("pf.id"), "left")
        .join(country_df.alias("c"), col("pf.country_fk") == col("c.id"), "left")
        .select(
            col("ad.date").alias("date"),
            col("dw.day_of_week"),
            col("at.time").alias("time"),
            col("pd.name").alias("part_of_day"),

            col("a.latitude"),
            col("a.longitude"),
            col("a.urban_or_rural_area"),

            col("a.number_of_casualties"),
            col("a.number_of_vehicles"),
            col("a.accident_severity"),
            col("a.speed_limit"),

            trim(col("lc.name")).alias("light"),
            trim(col("wc.name")).alias("weather"),
            trim(col("rsc.name")).alias("road_surface"),

            trim(col("vt.name")).alias("vehicle_type"),
            col("vt.wheels"),
            col("vt.capacity"),
            trim(col("vc.name")).alias("category"),

            trim(col("jd.name")).alias("junction_detail"),
            trim(col("rt.name")).alias("road_type"),

            trim(col("la.name")).alias("local_authority"),
            trim(col("pf.name")).alias("police_force"),
            trim(col("c.name")).alias("country"),
            col("c.population"),

            trim(col("s.season")).alias("season")
        )
    )

    # print("MySQL sales data count:", enriched_mysql_sales.count())

    # Normalize CSV sales (if any)
    if csv_accident_df:
        cleaned_csv_accident = (
            csv_accident_df
            .withColumn("date", initcap(trim(col("date"))))
            .withColumn("day_of_week", initcap(trim(col("day_of_week"))))
            .withColumn("time", initcap(trim(col("time"))))
            .withColumn("part_of_day", initcap(trim(col("part_of_day"))))
            .withColumn("latitude", initcap(trim(col("latitude"))))
            .withColumn("longitude", initcap(trim(col("longitude"))))
            .withColumn("urban_or_rural_area", initcap(trim(col("urban_or_rural_area"))))
            .withColumn("number_of_casualties", initcap(trim(col("number_of_casualties"))))
            .withColumn("number_of_vehicles", initcap(trim(col("number_of_vehicles"))))
            .withColumn("accident_severity", initcap(trim(col("accident_severity"))))
            .withColumn("light", initcap(trim(col("light_conditions"))))
            .withColumn("weather", initcap(trim(col("weather_conditions"))))
            .withColumn("road_surface", initcap(trim(col("road_surface_conditions"))))
            .withColumn("vehicle_type", initcap(trim(col("vehicle_type"))))
            .withColumn("wheels", col("revenue").cast("wheels"))
            .withColumn("capacity", initcap(trim(col("capacity"))))
            .withColumn("category", initcap(trim(col("category"))))
            .withColumn("junction_detail", initcap(trim(col("junction_detail"))))
            .withColumn("road_type", initcap(trim(col("road_type"))))
            .withColumn("local_authority", initcap(trim(col("local_authority"))))
            .withColumn("police_force", initcap(trim(col("police_force"))))
            .withColumn("country", initcap(trim(col("country"))))
            .withColumn("population", initcap(trim(col("population"))))
            .withColumn("season", initcap(trim(col("season"))))
            .select("date", "day_of_week", "time", "part_of_day", "latitude", "longitude", "urban_or_rural_area", "number_of_casualties", 
                    "number_of_vehicles", "accident_severity", "light", "weather", "road_surface", "vehicle_type", "wheels", "capacity", 
                    "category", "junction_detail", "road_type", "local_authority", "police_force", "country", "population", "season") 
        )
    else:
        cleaned_csv_accident = None

    # print("CSV sales data count:", cleaned_csv_sales.count() if cleaned_csv_sales else 0)

    # Merge MySQL and CSV sales
    combined_accident = enriched_mysql_accident
    if cleaned_csv_accident:
        combined_accident = combined_accident.unionByName(cleaned_csv_accident)

    # Join with dimensions
    fact_df = (
        combined_accident.alias("a")
        .join(dim_location_df.alias("l"), 
              (col("a.latitude") == col("l.latitude")) & (col("a.longitude") == col("l.longitude")), "left")
        .join(dim_road_df.alias("r"), col("a.road_type") == col("r.road_type"), "left")
        .join(dim_conditions_df.alias("c"),
              (col("a.light") == col("c.light")) & (col("a.weather") == col("c.weather")) & (col("a.road_surface") == col("c.road_surface")), 
              "left")
        .join(dim_time_df.alias("t"),
              (col("a.time") == col("t.time")) & (col("a.date") == col("t.date")),
              "left")
        .join(dim_vehicle_df.alias("v"), col("a.vehicle_type") == col("v.vehicle_type"), "left")
        .select(
            col("a.number_of_casualties"),
            col("a.number_of_vehicles"),
            col("a.accident_severity"),
            col("t.time_tk"),
            col("l.location_tk"),
            col("c.conditions_tk"), 
            col("v.vehicle_tk"),
            col("r.road_tk")
        )
    )


    fact_df = fact_df.withColumn(
        "fact_sales_tk",
        row_number().over(Window.orderBy("t.time_tk", "l.location_tk", "c.conditions_tk", "v.vehicle_tk", "r.road_tk"))
    )

    # print("Final fact sales row count:", fact_df.count())
    assert fact_df.count() == 237958, "Number of accident records from step one of the project."
    return fact_df