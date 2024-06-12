from dotenv import dotenv_values
import requests
import json
import datetime
import pandas as pd

import pandas as pd
from sqlalchemy import create_engine, text, types
from sqlalchemy.dialects.postgresql import JSON as postgres_json

config = dotenv_values('token.env')

# define variables for the login
port = config['postgres_port']
host = config['postgres_IP']
username = config['postgres_user']
password = config['postgres_PW']
db_climate = config['postgres_DB']

weather_api_key = config['weatherapi']


weather_dict = {'extracted_at':[], 'extracted_data':[]}
locations = ['Berlin', 'Tripolis', 'Bangui', 'Cape Town'] 

url = f'postgresql://{username}:{password}@{host}:{port}/{db_climate}'

engine = create_engine(url, echo=True)

for city in locations:
     for day in pd.date_range(start='09/10/2023', end='09/13/2023'):
        requested_day = day.date()
        print(city, requested_day) # check the values passed to the variables

        api_url_2 = f'http://api.weatherapi.com/v1/history.json?key={weather_api_key}&q={city}&dt={requested_day}'
        response_loop = requests.request("GET", api_url_2)

        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f") 

        weather_dict['extracted_at'].append(dt_str)

        weather_dict['extracted_data'].append(json.loads(response_loop.text))

        if response_loop.status_code == 200:
            print(f'attempt for {day.date()} in {city} resulted in {response_loop.status_code}', end='\r')
        else:
            print(f'for date: {day.date()} and city: {city} status code {response_loop.status_code} -> research error')

        weather_dict_df = pd.DataFrame(weather_dict)
        dtype_dict = {'extracted_at':types.DateTime, 'extracted_data':postgres_json}
        weather_dict_df.to_sql('weather_raw', engine, if_exists='replace', dtype=dtype_dict)


# ## Serialize the string using the json module  
# with open('json_example.json', mode='w') as f:
#     f.write(json_data)