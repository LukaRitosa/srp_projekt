from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/accidents" # Adjust the connection string as needed
engine = create_engine(DATABASE_URL, echo=True) # Enable echo to see the generated SQL statements in the console
Session = sessionmaker(bind=engine) 
session = Session() # Create a session to interact with the database
Base = declarative_base() # Base class for our models

# Define Dimensional Model Tables
class DimTime(Base):
    __tablename__ = 'dim_time'
    __table_args__ = {'schema': 'accidents'}

    time_tk = Column(BigInteger, primary_key=True)

    time = Column(String(45), nullable=False)
    part_of_day= Column(String(45), nullable=False)
    date= Column(DateTime, nullable=False)
    day_of_week= Column(String(45), nullable=False)
    season= Column(String(45), nullable=False)


class DimLocation(Base):
    __tablename__ = 'dim_location'
    __table_args__ = {'schema': 'accidents'}

    location_tk = Column(BigInteger, primary_key=True)

    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    urban_or_rural_area = Column(String(45), nullable=False)
    local_authority= Column(String(75), nullable=False)
    police_force= Column(String(75), nullable=False)
    country_id= Column(Integer, index=True, nullable=False)
    country= Column(String(75), nullable=False)
    population= Column(Integer, nullable=False)

    date_from = Column(DateTime, nullable=False)
    date_to = Column(DateTime)
    is_current = Column(Boolean, nullable=False)


class DimConditions(Base):
    __tablename__ = 'dim_conditions'
    __table_args__ = {'schema': 'accidents'}

    conditions_tk = Column(BigInteger, primary_key=True)

    weather= Column(String(75), nullable=False)
    road_surface= Column(String(75), nullable=False)
    light= Column(String(75), nullable=False)

class DimVehicle(Base):
    __tablename__ = 'dim_vehicle'
    __table_args__ = {'schema': 'accidents'}

    vehicle_tk = Column(BigInteger, primary_key=True)

    vehicle_type= Column(String(75), unique=True, nullable=False)
    capacity= Column(Integer, nullable=False)
    wheels= Column(Integer, nullable=False)
    category= Column(String(75), nullable=False)


class DimRoad(Base):
    __tablename__ = 'dim_road'
    __table_args__ = {'schema': 'accidents'}

    road_tk= Column(BigInteger, primary_key=True)

    junction_detail= Column(String(45), nullable=False)
    road_type= Column(String(75), nullable=False)
    speed_limit= Column(Integer, nullable=False)

class FactAccidents(Base):
    __tablename__ = 'fact_accident'
    __table_args__ = {'schema': 'accidents'}

    accidents_tk = Column(BigInteger, primary_key=True)

    number_of_casualties= Column(Integer, nullable=False)
    number_of_vehicles= Column(Integer, nullable=False)
    accident_severity= Column(String(45), nullable=False)

    time_tk = Column(BigInteger, ForeignKey('accidents.dim_time.time_tk'))
    location_tk = Column(BigInteger, ForeignKey('accidents.dim_location.location_tk'))
    conditions_tk = Column(BigInteger, ForeignKey('accidents.dim_conditions.conditions_tk'))
    vehicle_tk = Column(BigInteger, ForeignKey('accidents.dim_vehicle.vehicle_tk'))
    road_tk = Column(BigInteger, ForeignKey('accidents.dim_road.road_tk'))

Base.metadata.create_all(engine)

print("Dimensional model tables created successfully!")
