import configparser
from pathlib import Path
import requests
import json
from datetime import date, datetime, timedelta
import psycopg2
from queries import create_weather_table, create_weather_schema, insert_weather_table

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

api_key = config['KEYS']['API_KEY']

yesterday_date = datetime.now().date() - timedelta(days=1)
formatted_yesterday_date = yesterday_date.strftime("%Y-%m-%d")



def get_content(url, param):
    response = requests.get(url, params=param)
    if response.status_code == 200:
        return json.loads(response.content)
    else:
        print(f"Request completed with Error. Response Code : {response.status_code}")
        return None

def get_hourly_weather_report(date=formatted_yesterday_date,city='Dubai'):

    base_url = 'https://api.weatherapi.com/v1/history.json'
    param = {'key': api_key, 'q': city, 'dt': date}

    weather_search_request = get_content(url=base_url, param=param)

    longitute = weather_search_request['location']['lon']
    latitude = weather_search_request['location']['lat']
    hourly_weather_report = (weather_search_request['forecast']['forecastday'][0]['hour']) if weather_search_request is not None else []

    for hour in hourly_weather_report: #go through each record and transform them
        hour["time"] = datetime.strptime(hour['time'], "%Y-%m-%d %H:%M") #changes the datatype of the time column
        hour["longitute"] = longitute #add longitute to each record
        hour["latitude"] = latitude #add latitude to each record

    return hourly_weather_report

conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DATABASE'].values()))
cur = conn.cursor()
#cur.execute(create_weather_schema)
#cur.execute(create_weather_table)


#The code below generates the dates in YYYY-MM-DD format from a year ago till 1 day before the current day
#today = date.today()
#one_year_ago = today - timedelta(days=365)
# Loop through dates from a year ago to yesterday (inclusive)
#date_list = []
#for day in range((today - one_year_ago).days + 1):
#  date_to_add = one_year_ago + timedelta(days=day)
#  date_list.append(date_to_add.strftime("%Y-%m-%d"))


#def to_string(data):
#    return [str(value) for value in data.values()]

#for date in date_list:
#    queries = [insert_weather_table.format(*to_string(result)) for result in get_hourly_weather_report(date,"Dubai")]
#    query_to_execute = "BEGIN; \n" + '\n'.join(queries) + "\nCOMMIT;"
#    cur.execute(query_to_execute)


sql = f"COPY weather_reports.Dubai_weather_reports TO STDOUT WITH CSV HEADER"
with open("dubaiWR.csv", 'w') as file:
    cur.copy_expert(sql, file)

# Close connection
conn.close()