import pandas as pd
from datetime import date 

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "data/Road Accident Data.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

df= df.drop(columns="Carriageway_Hazards")
df= df.drop(columns="Accident_Index")
df= df.drop(columns="Junction_Control")
df = df.drop(df[df["Vehicle_Type"] == "Other vehicle"].index)
df["Accident_Severity"] = df["Accident_Severity"].replace("Fetal", "Fatal")

df["Accident Date"]= pd.to_datetime(df["Accident Date"], format="%m/%d/%Y")
df["Time"]= pd.to_datetime(df["Time"], format="%H:%M")

df = df.dropna() # Brisanje redaka s nedostajućim vrijednostima
df.columns = df.columns.str.lower() # Pretvori sve nazive stupaca u mala slova
df.columns = df.columns.str.replace(' ', '_')  # Zamjena razmaka u nazivima stupaca s donjom crtom

df = df.rename(columns={
    'local_authority_(district)': 'local_authority'
})



print(df.columns.values)

# Count if there are duplicates
duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}") # Ispis broja duplikata

df = df.drop_duplicates()

duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}") # Ispis broja duplikata


print("CSV size after: ", df.shape) # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head()) # Ispis prvih redaka dataframe-a

fix_local_authority = {
    "Crewe and ntwich": "Crewe and Nantwich",
    "Stevege": "Stevenage",
    "Blaeu Gwent": "Blaenau Gwent",
    "Clackmannshire": "Clackmannanshire",
    "North Larkshire": "North Lanarkshire",
    "South Larkshire": "South Lanarkshire"
}

df["local_authority"] = df["local_authority"].replace(fix_local_authority)


# Wales:

#     North Wales
#     Gwent
#     South Wales
#     Dyfed-Powys

# Scotland:

#     Northern
#     Grampian
#     Tayside
#     Fife
#     Lothian and Borders
#     Central
#     Strathclyde
#     Dumfries and Galloway


wales = ["North Wales", "Gwent", "South Wales", "Dyfed-Powys"]

scotland = [
    "Northern", "Grampian", "Tayside", "Fife",
    "Lothian and Borders", "Central", "Strathclyde",
    "Dumfries and Galloway"
]

def map_country(police_force):
    if police_force in wales:
        return "Wales"
    elif police_force in scotland:
        return "Scotland"
    else:
        return "England"

df["country"]= df["police_force"].apply(map_country)

print(df["country"].value_counts())
print(df[df["country"].isna()])



def map_season(d):
    year= d.year

    spring = date(year, 3, 20)
    summer = date(year, 6, 21)
    autumn = date(year, 9, 23)
    winter = date(year, 12, 21)

    d= d.date()

    if spring <= d < summer:
        return "Spring"
    elif summer <= d < autumn:
        return "Summer"
    elif autumn <= d < winter:
        return "Autumn"
    else:
        return "Winter"

df["season"] = df["accident_date"].apply(map_season)


print(df["season"].value_counts())
print(df[df["season"].isna()])




def map_part_of_day(time):
    hour= time.hour

    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"
    
df["part_of_day"]= df["time"].apply(map_part_of_day)


print(df["part_of_day"].value_counts())
print(df[df["part_of_day"].isna()])


df['time'] = df['time'].dt.time

vehicle_dim = {
    "Car": {"wheels": 4, "capacity": 5, "category": "light"},
    "Taxi/Private hire car": {"wheels": 4, "capacity": 4, "category": "light"},
    "Motorcycle over 500cc": {"wheels": 2, "capacity": 2, "category": "motorcycle"},
    "Motorcycle over 125cc and up to 500cc": {"wheels": 2, "capacity": 2, "category": "motorcycle"},
    "Motorcycle 125cc and under": {"wheels": 2, "capacity": 1, "category": "motorcycle"},
    "Motorcycle 50cc and under": {"wheels": 2, "capacity": 1, "category": "motorcycle"},
    "Van / Goods 3.5 tonnes mgw or under": {"wheels": 4, "capacity": 3, "category": "commercial"},
    "Goods over 3.5t. and under 7.5t": {"wheels": 4, "capacity": 3, "category": "commercial"},
    "Goods 7.5 tonnes mgw and over": {"wheels": 6, "capacity": 4, "category": "commercial"},
    "Bus or coach (17 or more pass seats)": {"wheels": 6, "capacity": 50, "category": "bus"},
    "Minibus (8 - 16 passenger seats)": {"wheels": 4, "capacity": 16, "category": "bus"},
    "Pedal cycle": {"wheels": 2, "capacity": 1, "category": "bike"},
    "Ridden horse": {"wheels": 0, "capacity": 1, "category": "animal"},
    "Agricultural vehicle": {"wheels": 4, "capacity": 2, "category": "commercial"},
}

vehicle_df = pd.DataFrame.from_dict(vehicle_dim, orient="index").reset_index()
vehicle_df.columns = ["vehicle_type", "wheels", "capacity", "category"]

df = df.merge(vehicle_df, on="vehicle_type", how="left")

print(df["wheels"].value_counts())
print(df[df["wheels"].isna()])

print(df["capacity"].value_counts())
print(df[df["capacity"].isna()])

print(df["category"].value_counts())
print(df[df["category"].isna()])




# Random dijeljenje skupa podataka na dva dijela 80:20 (trebat će nam kasnije)
df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

# Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df.to_csv("checkpoint_2/processed/Road_Accident_Data_PROCESSED.csv", index=False) # Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df20.to_csv("checkpoint_2/processed/Road_Accident_Data_PROCESSED_20.csv", index=False) # Spremanje 20% skupa podataka u novu CSV datoteku



'''
CSV size before:  (307973, 21)


Number of duplicates: 5
Number of duplicates: 0


CSV size after:  (298042, 18)


  accident_date day_of_week          junction_detail accident_severity  ...                time urban_or_rural_area  weather_conditions           vehicle_type
0    2021-01-01    Thursday  T or staggered junction           Serious  ... 1900-01-01 15:11:00               Urban  Fine no high winds                    Car
1    2021-01-05      Monday               Crossroads           Serious  ... 1900-01-01 10:59:00               Urban  Fine no high winds  Taxi/Private hire car
2    2021-01-04      Sunday  T or staggered junction            Slight  ... 1900-01-01 14:19:00               Urban  Fine no high winds  Taxi/Private hire car
3    2021-01-05      Monday  T or staggered junction           Serious  ... 1900-01-01 08:10:00               Urban               Other  Motorcycle over 500cc
4    2021-01-06     Tuesday               Crossroads           Serious  ... 1900-01-01 17:25:00               Urban  Fine no high winds                    Car

[5 rows x 18 columns]


['accident_date' 'day_of_week' 'junction_detail' 'accident_severity'
 'latitude' 'light_conditions' 'local_authority' 'longitude'
 'number_of_casualties' 'number_of_vehicles' 'police_force'
 'road_surface_conditions' 'road_type' 'speed_limit' 'time'
 'urban_or_rural_area' 'weather_conditions' 'vehicle_type']


country
England     273635
Wales        13040
Scotland     11367
Name: count, dtype: int64


Empty DataFrame
Columns: [accident_date, day_of_week, junction_detail, accident_severity, latitude, light_conditions, local_authority, longitude, number_of_casualties, number_of_vehicles, police_force, road_surface_conditions, road_type, speed_limit, time, urban_or_rural_area, weather_conditions, vehicle_type, country]
Index: []


season
Autumn    79578
Summer    78759
Spring    75137
Winter    64568
Name: count, dtype: int64


Empty DataFrame
Columns: [accident_date, day_of_week, junction_detail, accident_severity, latitude, light_conditions, local_authority, longitude, number_of_casualties, number_of_vehicles, police_force, road_surface_conditions, road_type, speed_limit, time, urban_or_rural_area, weather_conditions, vehicle_type, country, season]        
Index: []


part_of_day
Afternoon    102011
Morning       85666
Evening       72794
Night         37571
Name: count, dtype: int64
Empty DataFrame


Columns: [accident_date, day_of_week, junction_detail, accident_severity, latitude, light_conditions, local_authority, longitude, number_of_casualties, number_of_vehicles, police_force, road_surface_conditions, road_type, speed_limit, time, urban_or_rural_area, weather_conditions, vehicle_type, country, season, part_of_day]
Index: []


wheels
4.0    254675
2.0     11024
6.0      8497
0.0         3
Name: count, dtype: int64


       accident_date day_of_week                      junction_detail accident_severity   latitude  ...  season part_of_day  wheels  capacity  category
44        2021-01-30      Friday                           Roundabout            Slight  51.504672  ...  Winter     Morning     NaN       NaN       NaN
46        2021-01-29    Thursday              T or staggered junction            Slight  51.480175  ...  Winter   Afternoon     NaN       NaN       NaN
72        2021-03-04   Wednesday                      Mini-roundabout            Slight  51.517796  ...  Winter   Afternoon     NaN       NaN       NaN
74        2021-02-17     Tuesday              T or staggered junction           Serious  51.497103  ...  Winter     Evening     NaN       NaN       NaN
88        2021-03-09      Monday                           Crossroads            Slight  51.490315  ...  Winter   Afternoon     NaN       NaN       NaN
...              ...         ...                                  ...               ...        ...  ...     ...         ...     ...       ...       ...
297979    2022-07-14   Wednesday  Not at junction or within 20 metres            Slight  58.997871  ...  Summer     Evening     NaN       NaN       NaN
297989    2022-12-05      Sunday  Not at junction or within 20 metres            Slight  58.997002  ...  Autumn       Night     NaN       NaN       NaN
298010    2022-10-26     Tuesday              T or staggered junction            Slight  60.148990  ...  Autumn       Night     NaN       NaN       NaN
298027    2022-01-16    Saturday  Not at junction or within 20 metres            Slight  56.880265  ...  Winter       Night     NaN       NaN       NaN
298028    2022-01-21    Thursday  Not at junction or within 20 metres            Slight  56.884448  ...  Winter   Afternoon     NaN       NaN       NaN

[23843 rows x 24 columns]


capacity
5.0     233960
3.0      15312
2.0      10959
50.0      8497
4.0       5403
1.0         68


Name: count, dtype: int64
       accident_date day_of_week                      junction_detail accident_severity   latitude  ...  season part_of_day  wheels  capacity  category
44        2021-01-30      Friday                           Roundabout            Slight  51.504672  ...  Winter     Morning     NaN       NaN       NaN
46        2021-01-29    Thursday              T or staggered junction            Slight  51.480175  ...  Winter   Afternoon     NaN       NaN       NaN
72        2021-03-04   Wednesday                      Mini-roundabout            Slight  51.517796  ...  Winter   Afternoon     NaN       NaN       NaN
74        2021-02-17     Tuesday              T or staggered junction           Serious  51.497103  ...  Winter     Evening     NaN       NaN       NaN
88        2021-03-09      Monday                           Crossroads            Slight  51.490315  ...  Winter   Afternoon     NaN       NaN       NaN
...              ...         ...                                  ...               ...        ...  ...     ...         ...     ...       ...       ...
297979    2022-07-14   Wednesday  Not at junction or within 20 metres            Slight  58.997871  ...  Summer     Evening     NaN       NaN       NaN
297989    2022-12-05      Sunday  Not at junction or within 20 metres            Slight  58.997002  ...  Autumn       Night     NaN       NaN       NaN
298010    2022-10-26     Tuesday              T or staggered junction            Slight  60.148990  ...  Autumn       Night     NaN       NaN       NaN
298027    2022-01-16    Saturday  Not at junction or within 20 metres            Slight  56.880265  ...  Winter       Night     NaN       NaN       NaN
298028    2022-01-21    Thursday  Not at junction or within 20 metres            Slight  56.884448  ...  Winter   Afternoon     NaN       NaN       NaN

[23843 rows x 24 columns]


category
light         239363
commercial     15312
motorcycle     10959
bus             8497
bike              65
animal             3
Name: count, dtype: int64


       accident_date day_of_week                      junction_detail accident_severity   latitude  ...  season part_of_day  wheels  capacity  category
44        2021-01-30      Friday                           Roundabout            Slight  51.504672  ...  Winter     Morning     NaN       NaN       NaN
46        2021-01-29    Thursday              T or staggered junction            Slight  51.480175  ...  Winter   Afternoon     NaN       NaN       NaN
72        2021-03-04   Wednesday                      Mini-roundabout            Slight  51.517796  ...  Winter   Afternoon     NaN       NaN       NaN
74        2021-02-17     Tuesday              T or staggered junction           Serious  51.497103  ...  Winter     Evening     NaN       NaN       NaN
88        2021-03-09      Monday                           Crossroads            Slight  51.490315  ...  Winter   Afternoon     NaN       NaN       NaN
...              ...         ...                                  ...               ...        ...  ...     ...         ...     ...       ...       ...
297979    2022-07-14   Wednesday  Not at junction or within 20 metres            Slight  58.997871  ...  Summer     Evening     NaN       NaN       NaN
297989    2022-12-05      Sunday  Not at junction or within 20 metres            Slight  58.997002  ...  Autumn       Night     NaN       NaN       NaN
298010    2022-10-26     Tuesday              T or staggered junction            Slight  60.148990  ...  Autumn       Night     NaN       NaN       NaN
298027    2022-01-16    Saturday  Not at junction or within 20 metres            Slight  56.880265  ...  Winter       Night     NaN       NaN       NaN
298028    2022-01-21    Thursday  Not at junction or within 20 metres            Slight  56.884448  ...  Winter   Afternoon     NaN       NaN       NaN

[23843 rows x 24 columns]


CSV size 80:  (238434, 24)
CSV size 20:  (59608, 24)
'''