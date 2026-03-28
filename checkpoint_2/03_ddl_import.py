# Imports
import pandas as pd
import json
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date, insert
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import List, Dict, Any


# Putanja do predprocesirane CSV datoteke
CSV_FILE_PATH = "checkpoint_2/processed/Road_Accident_Data_PROCESSED_20.csv"

# Učitavanje CSV datoteke u dataframe
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")  # Print dataset size
print(df.head())  # Preview first few rows

# Database Connection
Base = declarative_base()

# Definiranje sheme baze podataka
# --------------------------------------------------------------

class LightConditions(Base):
    __tablename__ = 'light_conditions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class WeatherConditions(Base):
    __tablename__ = 'weather_conditions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class RoadSurfaceConditions(Base):
    __tablename__ = 'road_surface_conditions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)

class VehicleType(Base):
    __tablename__ = 'vehicle_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)

class JunctionDetail(Base):
    __tablename__ = 'junction_detail'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

# class JunctionControl(Base):
#     __tablename__ = 'junction_control'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     name = Column(String(75), nullable=False, unique=True)

class RoadType(Base):
    __tablename__ = 'road_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name= Column(String(75), nullable=False, unique=True)

class Country(Base):
    __tablename__ = 'country'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name= Column(String(75), nullable=False, unique=True)
    population= Column(Integer, nullable=False)

class PoliceForce(Base):
    __tablename__ = 'police_force'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name= Column(String(75), unique=True, nullable=False)
    country_fk = Column(Integer, ForeignKey('country.id'))

class LocalAuthority(Base):
    __tablename__ = 'local_authority'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)
    police_force_fk = Column(Integer, ForeignKey('police_force.id'))

class Accident(Base):
    __tablename__ = 'accident'
    id = Column(Integer, primary_key=True, autoincrement=True)
    accident_date = Column(Date, nullable=False)
    day_of_week = Column(String(45), nullable=False)
    time = Column(String(45), nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    urban_or_rural_area = Column(String(45), nullable=False)
    number_of_casualties = Column(Integer, nullable=False)
    number_of_vehicles = Column(Integer, nullable=False)
    accident_severity  = Column(String(45), nullable=False)
    speed_limit = Column(Integer, nullable=False)
    local_authority_fk = Column(Integer, ForeignKey('local_authority.id'))
    # junction_control_fk = Column(Integer, ForeignKey('junction_control.id'))
    road_type_fk = Column(Integer, ForeignKey('road_type.id'))
    junction_detail_fk = Column(Integer, ForeignKey('junction_detail.id'))
    vehicle_type_fk = Column(Integer, ForeignKey('vehicle_type.id'))
    road_surface_conditions_fk = Column(Integer, ForeignKey('road_surface_conditions.id'))
    weather_conditions_fk = Column(Integer, ForeignKey('weather_conditions.id'))
    light_conditions_fk = Column(Integer, ForeignKey('light_conditions.id'))

# Database Connection
engine = create_engine('mysql+pymysql://root:root@localhost:3306/accidents', echo=False)
Base.metadata.drop_all(engine)  # Brisanje postojećih tablica
Base.metadata.create_all(engine)  # Stvaranje tablica

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Otvori novu sesiju

# --------------------------------------------------------------
# Import podataka
# --------------------------------------------------------------

# **1. Umetanje osvjetljenja**
light_conditions = df[['light_conditions']].drop_duplicates().rename(columns={'light_conditions': 'name'}) 
light_conditions_list = [{str(k): v for k, v in row.items()} for row in light_conditions.to_dict(orient="records")] 

session.execute(insert(LightConditions), light_conditions_list) # Bulk insert
session.commit() 

light_conditions_map = {c.name: c.id for c in session.query(LightConditions).all()} 


# **2. Umetanje weather_conditions**
weather_conditions = df[['weather_conditions']].drop_duplicates().rename(columns={'weather_conditions': 'name'}) 
weather_conditions_list = [{str(k): v for k, v in row.items()} for row in weather_conditions.to_dict(orient="records")]

session.execute(insert(WeatherConditions), weather_conditions_list)
session.commit()

weather_conditions_map = {om.name: om.id for om in session.query(WeatherConditions).all()} 


# **3. Umetanje road_surface_conditions**
road_surface_conditions = df[['road_surface_conditions']].drop_duplicates().rename(columns={'road_surface_conditions': 'name'}) 
road_surface_conditions_list = [{str(k): v for k, v in row.items()} for row in road_surface_conditions.to_dict(orient="records")] 


session.execute(insert(RoadSurfaceConditions), road_surface_conditions_list) # Bulk insert
session.commit()

road_surface_conditions_map = {rt.name: rt.id for rt in session.query(RoadSurfaceConditions).all()} 


# **4. Umetanje vehicle_type**
vehicle_type = df[['vehicle_type']].drop_duplicates().rename(columns={'vehicle_type': 'name'}) 
vehicle_type_list = [{str(k): v for k, v in row.items()} for row in vehicle_type.to_dict(orient="records")] 
session.execute(insert(VehicleType), vehicle_type_list) # Bulk insert
session.commit()

vehicle_type_map = {pl.name: pl.id for pl in session.query(VehicleType).all()} 


# **5. Umetanje junction_detail**
junction_detail = df[['junction_detail']].drop_duplicates().rename(columns={'junction_detail': 'name'}) 
junction_detail_list = [{str(k): v for k, v in row.items()} for row in junction_detail.to_dict(orient="records")] 
session.execute(insert(JunctionDetail), junction_detail_list) 
session.commit()

junction_detail_map = {pl.name: pl.id for pl in session.query(JunctionDetail).all()} 


# **5. Umetanje junction_control**
# junction_control = df[['junction_control']].drop_duplicates().rename(columns={'junction_control': 'name'}) 
# junction_control_list = [{str(k): v for k, v in row.items()} for row in junction_control.to_dict(orient="records")] 
# session.execute(insert(JunctionControl), junction_control_list) 
# session.commit()

# junction_control_map = {pl.name: pl.id for pl in session.query(JunctionControl).all()} 



# road_type
road_type = df[['road_type']].drop_duplicates().rename(columns={'road_type': 'name'}) 
road_type_list = [{str(k): v for k, v in row.items()} for row in road_type.to_dict(orient="records")] 
session.execute(insert(RoadType), road_type_list) # Bulk insert
session.commit()

road_type_map = {pl.name: pl.id for pl in session.query(RoadType).all()} 

# country
country_list = [
    {"name": "Scotland", "population": 5550000},
    {"name": "Wales", "population": 3107500},
    {"name": "England", "population": 56489800}
]
session.execute(insert(Country), country_list) 
session.commit()

country_map = {c.name: c.id for c in session.query(Country).all()} 


# police_force

police_force = df[['police_force', 'country']].drop_duplicates() 
police_force['country_fk']= police_force['country'].map(country_map)
police_force= police_force.rename(columns={'police_force': 'name'}).drop(columns='country')
police_list= [{str(k): v for k, v in row.items()} for row in police_force.to_dict(orient="records")] 

session.execute(insert(PoliceForce), police_list) 
session.commit()

police_force_map = {pt.name: pt.id for pt in session.query(PoliceForce).all()} 


# local_authority_(district)
local_authority= df[['local_authority', 'police_force']].drop_duplicates()
local_authority['police_force_fk'] = local_authority['police_force'].map(police_force_map)
local_authority= local_authority.rename(columns={'local_authority': 'name'}).drop(columns='police_force')
local_authority_list= [{str(k): v for k, v in row.items()} for row in local_authority.to_dict(orient="records")] 

session.execute(insert(LocalAuthority), local_authority_list) 
session.commit()

local_authority_map= {d.name: d.id for d in session.query(LocalAuthority).all()}


# Accident
accident_data=df[[ 'accident_date', 'day_of_week', 'time', 
    'latitude', 'longitude', 'urban_or_rural_area', 'number_of_casualties', 
    'number_of_vehicles', 'accident_severity', 'speed_limit',
    # 'junction_control', 
    'junction_detail', 'light_conditions', 
    'local_authority', 'road_surface_conditions', 
    'road_type', 'weather_conditions', 'vehicle_type']].copy()

'''
[ 
 'junction_control',
 'junction_detail', 
 'light_conditions', 
 'local_authority', 
 'road_surface_conditions', 
 'road_type', 
 'weather_conditions', 
 'vehicle_type']

 '''

accident_data['local_authority_fk']= accident_data['local_authority'].map(local_authority_map)
# accident_data['junction_control_fk']= accident_data['junction_control'].map(junction_control_map)
accident_data['road_type_fk']= accident_data['road_type'].map(road_type_map)
accident_data['junction_detail_fk']= accident_data['junction_detail'].map(junction_detail_map)
accident_data['vehicle_type_fk']= accident_data['vehicle_type'].map(vehicle_type_map)
accident_data['road_surface_conditions_fk']= accident_data['road_surface_conditions'].map(road_surface_conditions_map)
accident_data['weather_conditions_fk']= accident_data['weather_conditions'].map(weather_conditions_map)
accident_data['light_conditions_fk']= accident_data['light_conditions'].map(light_conditions_map)

accident_list= [{str(k): v for k, v in row.items()} for row in accident_data.drop(columns=[
        # 'junction_control',
        'junction_detail', 
        'light_conditions', 
        'local_authority', 
        'road_surface_conditions', 
        'road_type', 
        'weather_conditions', 
        'vehicle_type'
    ]).to_dict(orient="records")]

session.execute(insert(Accident), accident_list)
session.commit()

print("Data imported successfully!")

'''
OUTPUT:

CSV size: (60098, 19)

  accident_date day_of_week                      junction_detail accident_severity  ...  urban_or_rural_area  weather_conditions vehicle_type  country
0    2022-08-15      Sunday  Not at junction or within 20 metres            Slight  ...                Urban  Fine no high winds          Car  England
1    2021-08-20    Thursday  Not at junction or within 20 metres            Slight  ...                Rural  Fine no high winds          Car  England
2    2021-12-17    Thursday                       Other junction            Slight  ...                Urban  Fine no high winds          Car  England
3    2021-07-14     Tuesday              T or staggered junction            Slight  ...                Urban  Fine no high winds          Car  England
4    2022-01-15      Friday  Not at junction or within 20 metres            Slight  ...                Rural  Fine no high winds          Car  England

[5 rows x 19 columns]

Data imported successfully!
'''