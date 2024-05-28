create_weather_schema = """CREATE SCHEMA IF NOT EXISTS weather_reports;"""

create_weather_table = """
CREATE TABLE IF NOT EXISTS weather_reports.Dubai_weather_reports (
	timestamp TIMESTAMP  PRIMARY KEY,
	temp_c float,
	is_day int,
	wind_kph float,
	wind_degree int, 
	wind_dir varchar,
	pressure_mb float,
	precip_mm float,
	humidity float,
	feelslike_c float,
	windchill_c float,
	heatindex_c float,
	chance_of_rain float,
	vis_km float,
	gust_kph float ,
	uv float,
	longitute float,
	latitude float
);
"""

insert_weather_table = """INSERT INTO weather_reports.Dubai_weather_reports VALUES ('{}', '{}', '{}', '{}','{}', '{}', '{}', '{}','{}', '{}', '{}', '{}','{}', '{}', '{}', '{}','{}', '{}' )
                        ON CONFLICT (timestamp)
                        DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        temp_c = EXCLUDED.temp_C ,
                        is_day = EXCLUDED.is_day,
                        wind_kph = EXCLUDED.wind_kph,
                        wind_degree = EXCLUDED.wind_degree, 
                        wind_dir = EXCLUDED.wind_dir,
                        pressure_mb = EXCLUDED.pressure_mb,
                        precip_mm = EXCLUDED.precip_mm,
                        humidity = EXCLUDED.humidity,
                        feelslike_c = EXCLUDED.feelslike_c,
                        windchill_c = EXCLUDED.windchill_c,
                        heatindex_c = EXCLUDED.heatindex_c,
                        chance_of_rain = EXCLUDED.chance_of_rain,
                        vis_km = EXCLUDED.vis_km,
                        gust_kph  = EXCLUDED.gust_kph,
                        uv = EXCLUDED.uv,
                        longitute = EXCLUDED.longitute,
                        latitude = EXCLUDED.latitude;
                        """