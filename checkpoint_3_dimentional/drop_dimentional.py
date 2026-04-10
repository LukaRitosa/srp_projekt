from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/accidents"
engine = create_engine(DATABASE_URL, echo=True)

# Import your models here
from generate_star_shema import Base 

# Drop all tables defined in Base.metadata
Base.metadata.drop_all(engine)

print("✅ All dimensional model tables dropped.")