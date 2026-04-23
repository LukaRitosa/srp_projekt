from spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df

def extract_all_tables():
    return {
        "light_conditions": extract_table("light_conditions"),
        "weather_conditions": extract_table("weather_conditions"),
        "road_surface_conditions": extract_table("road_surface_conditions"),
        "vehicle_category": extract_table("vehicle_category"),
        "vehicle_type": extract_table("vehicle_type"),
        "junction_detail": extract_table("junction_detail"),
        "road_type": extract_table("road_type"),
        "country": extract_table("country"),
        "police_force": extract_table("police_force"),
        "local_authority": extract_table("local_authority"),
        "day_of_week": extract_table("day_of_week"),
        "season": extract_table("season"),
        "accident_date": extract_table("accident_date"),
        "part_of_day": extract_table("part_of_day"),
        "accident_time": extract_table("accident_time"),
        "accident": extract_table("accident"),
    }