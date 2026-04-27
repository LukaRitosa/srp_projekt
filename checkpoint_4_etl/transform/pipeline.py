from transform.dimensions.conditions_dim import transform_conditions_dim
from transform.dimensions.location_dim import transform_location_dim
from transform.facts.accident_fact import transform_accident_fact
from transform.dimensions.road_dim import transform_road_dim
from transform.dimensions.time_dim import transform_time_dim
from transform.dimensions.vehicle_dim import transform_vehicle_dim


def run_transformations(raw_data):
    # Transform dimensions
    conditions_dim = transform_conditions_dim(
        raw_data["accident"],
        raw_data["weather_conditions"],
        raw_data["road_surface_conditions"],
        raw_data["light_conditions"],
        csv_conditions_df=raw_data.get("csv_accident")
    )
    print("1️⃣ Conditions dimension complete")
       
    location_dim = transform_location_dim(
        raw_data["accident"],
        raw_data["local_authority"],
        raw_data["police_force"], 
        raw_data["country"],
        csv_location_df=raw_data.get("csv_accident")
    )
    print("2️⃣ Location dimension complete")
    
    road_dim = transform_road_dim(
        raw_data["accident"],
        raw_data["junction_detail"],
        raw_data["road_type"],
        csv_road_df=raw_data.get("csv_accident")
    )
    print("3️⃣ Road dimension complete")

    time_dim = transform_time_dim(
        raw_data["accident"],
        raw_data["accident_time"],
        raw_data["part_of_day"],
        raw_data["accident_date"],
        raw_data["day_of_week"],
        raw_data["season"],
        csv_time_df=raw_data.get("csv_accident")
    )
    print("4️⃣ Time dimension complete")
    
    vehicle_dim = transform_vehicle_dim(
        raw_data["vehicle_type"],
        raw_data["vehicle_category"],
        csv_vehicle_df=raw_data.get("csv_accident")
    )
    print("4️⃣ Vehicle dimension complete")

    accident_fact = transform_accident_fact(
        raw_data,
        conditions_dim,
        location_dim,
        road_dim,
        time_dim,
        vehicle_dim 
    )
    print("5️⃣ Sales fact table complete")

    return {
        "dim_time": time_dim,
        "dim_location": location_dim,
        "dim_conditions": conditions_dim,
        "dim_road": road_dim,
        "dim_vehicle": vehicle_dim,
        "fact_accident": accident_fact
    }