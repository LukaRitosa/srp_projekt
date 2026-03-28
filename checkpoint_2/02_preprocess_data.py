import pandas as pd

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "data/Road Accident Data.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

df= df.drop(columns="Carriageway_Hazards")
df= df.drop(columns="Accident_Index")
df= df.drop(columns="Junction_Control")

df["Accident Date"]= pd.to_datetime(df["Accident Date"], format="%m/%d/%Y")
df["Time"]= pd.to_datetime(df["Time"], format="%H:%M").dt.time

df = df.dropna() # Brisanje redaka s nedostajućim vrijednostima
df.columns = df.columns.str.lower() # Pretvori sve nazive stupaca u mala slova
df.columns = df.columns.str.replace(' ', '_')  # Zamjena razmaka u nazivima stupaca s donjom crtom

df = df.rename(columns={
    'local_authority_(district)': 'local_authority'
})

print("CSV size after: ", df.shape) # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head()) # Ispis prvih redaka dataframe-a


print(df.columns.values)

# Count if there are duplicates
duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}") # Ispis broja duplikata

df = df.drop_duplicates()

duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}") # Ispis broja duplikata

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
CSV size after:  (300495, 18)

  accident_date day_of_week          junction_detail accident_severity  ...      time urban_or_rural_area  weather_conditions           vehicle_type
0    2021-01-01    Thursday  T or staggered junction           Serious  ...  15:11:00               Urban  Fine no high winds                    Car
1    2021-01-05      Monday               Crossroads           Serious  ...  10:59:00               Urban  Fine no high winds  Taxi/Private hire car
2    2021-01-04      Sunday  T or staggered junction            Slight  ...  14:19:00               Urban  Fine no high winds  Taxi/Private hire car
3    2021-01-05      Monday  T or staggered junction           Serious  ...  08:10:00               Urban               Other  Motorcycle over 500cc
4    2021-01-06     Tuesday               Crossroads           Serious  ...  17:25:00               Urban  Fine no high winds                    Car

[5 rows x 18 columns]

['accident_date' 'day_of_week' 'junction_detail' 'accident_severity'
 'latitude' 'light_conditions' 'local_authority' 'longitude'
 'number_of_casualties' 'number_of_vehicles' 'police_force'
 'road_surface_conditions' 'road_type' 'speed_limit' 'time'
 'urban_or_rural_area' 'weather_conditions' 'vehicle_type']

Number of duplicates: 5
Number of duplicates: 0

 country
England     275781
Wales        13176
Scotland     11533
Name: count, dtype: int64

Empty DataFrame

Columns: [accident_date, day_of_week, junction_detail, accident_severity, latitude, light_conditions, local_authority, longitude, number_of_casualties, number_of_vehicles, police_force, road_surface_conditions, road_type, speed_limit, time, urban_or_rural_area, weather_conditions, vehicle_type, country]
Index: []

CSV size 80:  (240392, 19)
CSV size 20:  (60098, 19)
'''