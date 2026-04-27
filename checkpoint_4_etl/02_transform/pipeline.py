from dimensions.conditions_dim import transform_conditions_dim
from dimensions.location_dim import transform_location_dim
from facts.accident_fact import transform_accident_fact
from dimensions.road_dim import transform_road_dim
from dimensions.time_dim import transform_time_dim
from dimensions.vehicle_dim import transform_vehicle_dim


def run_transformations(raw_data):
    # Transform dimensions
    conditions_dim = transform_conditions_dim(
    raw_data["accident"],
    raw_data["weather_conditions"],
    raw_data["road_surface_conditions"],
    raw_data["light_conditions"],
    csv_sales_df=raw_data.get("csv_accident")
    )
    print("1️⃣ Conditions dimension complete")
       
    location_dim = transform_location_dim(
        raw_data["accident"],
        raw_data["local_authority_df"],
        raw_data["police_force_df"],
        raw_data["country_df"],
        csv_country_df=raw_data.get("csv_accident")
    )
    print("2️⃣ Location dimension complete")

    road_dim = transform_road_dim(
        raw_data["retailer_type"],
        csv_retailer_df=raw_data.get("csv_sales")
    )
    print("3️⃣ Road dimension complete")

    time_dim = transform_time_dim(
        raw_data["sales"],
        csv_date_df=raw_data.get("csv_sales")
    )
    print("4️⃣ Time dimension complete")

    vehicle_dim = transform_vehicle_dim(
        raw_data["sales"],
        csv_date_df=raw_data.get("csv_sales")
    )
    print("4️⃣ Vehicle dimension complete")

    accident_fact = transform_accident_fact(
        raw_data,
        product_dim,
        country_dim,
        retailer_dim,
        date_dim
    )
    print("5️⃣ Sales fact table complete")

    return {
        "dim_product": product_dim,
        "dim_country": country_dim,
        "dim_retailer": retailer_dim,
        "dim_date": date_dim,
        "fact_sales": fact_sales
    }