import unittest
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from pandas.testing import assert_frame_equal

class TestAccidentsDatabase(unittest.TestCase):
    def setUp(self):
        # Spajanje na bazu
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/accidents')
        self.connection = self.engine.connect()

        # Učitavanje CSV datoteke
        self.df = pd.read_csv("checkpoint_2/processed/Road_Accident_Data_PROCESSED.csv")

        # Dohvati podatke iz baze sa svim JOIN-ovima FK tablica
        query = """
        SELECT a.accident_date, a.day_of_week, a.time,
               a.latitude, a.longitude, a.urban_or_rural_area,
               a.number_of_casualties, a.number_of_vehicles,
               a.accident_severity, a.speed_limit,
               lc.name AS light_conditions,
               wc.name AS weather_conditions,
               rsc.name AS road_surface_conditions,
               vt.name AS vehicle_type,
               jd.name AS junction_detail,
               rt.name AS road_type,
               la.name AS local_authority
        FROM accident a
        JOIN light_conditions lc ON a.light_conditions_fk = lc.id
        JOIN weather_conditions wc ON a.weather_conditions_fk = wc.id
        JOIN road_surface_conditions rsc ON a.road_surface_conditions_fk = rsc.id
        JOIN vehicle_type vt ON a.vehicle_type_fk = vt.id
        JOIN junction_detail jd ON a.junction_detail_fk = jd.id
        JOIN road_type rt ON a.road_type_fk = rt.id
        JOIN local_authority la ON a.local_authority_fk = la.id
        ORDER BY a.id ASC
        """
        result = self.connection.execute(text(query))
        self.db_df = pd.DataFrame(result.fetchall())
        self.db_df.columns = list(result.keys())

    # Testiranje stupaca
    def test_columns(self):
        csv_columns = [
            'accident_date', 'day_of_week', 'time', 'latitude', 'longitude',
            'urban_or_rural_area', 'number_of_casualties', 'number_of_vehicles',
            'accident_severity', 'speed_limit', 'light_conditions', 'weather_conditions',
            'road_surface_conditions', 'vehicle_type', 'junction_detail',
            # 'junction_control', 
            'road_type', 'local_authority'
        ]
        self.assertListEqual(csv_columns, list(self.db_df.columns))

    # Testiranje podataka
    def test_dataframes(self):
        df_copy = self.df.copy()

        df_copy = df_copy[['accident_date', 'day_of_week', 'time', 'latitude', 'longitude',
                        'urban_or_rural_area', 'number_of_casualties', 'number_of_vehicles',
                        'accident_severity', 'speed_limit', 'light_conditions', 'weather_conditions',
                        'road_surface_conditions', 'vehicle_type', 'junction_detail',
                        # 'junction_control', 
                        'road_type', 'local_authority']]

        df_copy = df_copy.reset_index(drop=True)
        db_df_reset = self.db_df.reset_index(drop=True)

        df_copy['accident_date'] = pd.to_datetime(df_copy['accident_date'])
        db_df_reset['accident_date'] = pd.to_datetime(db_df_reset['accident_date'])

        assert_frame_equal(df_copy, db_df_reset)

    def tearDown(self):
        self.connection.close()


if __name__ == '__main__':
    unittest.main()


'''
..
----------------------------------------------------------------------
Ran 2 tests in 32.459s

OK
'''